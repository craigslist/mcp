# Copyright 2013 craigslist
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

package MCP::RequestModule::Logger;
use base 'MCP::RequestModule';
use IO::Socket::INET;
use Socket;
use Fcntl;
use strict;

# we want all of the data to fit into one packet
# so take the normal mtu of 1500 bytes and subtract
# 20 for the IP header and 8 for the UDP header
use constant MAX_UDP_DGRAM_SIZE => 1472;
# NB: if you change the log format, make sure to change it everywhere:
#  mcp/lib/perl/MCP/RequestModule/Logger.pm
#  mcplogger/lib/HTTP/MCP/Birler.pm
#  mcplogger/bin/mcplog_squid.pl
use constant LOG_FORMAT_VERSION => 4;
use constant LOG_KEYS           => (qw/
    timestamp local_ip remote_ip request_method request_host request_port
    request_proto status content_length served_by forward_host forward_port
    forward_host_confidence forward_retry_num timer_request timer_fixup
    timer_response timer_forward_connect timer_forward_request timer_cache_fetch
    timer_send wants_gzip x_forwarded_for throttled throttle_header
    session_cookie format_cookie
    request_uri user_agent referer_url
/);
# perhaps add forward_pool?  how would we stuff that the monitoring
# system, tho?

use constant DEBUG => 0;

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    config => $args->{'server'}->{'config'}->{'logger'},
    log_host_ip       => undef,
    log_host_portaddr => undef,
    log_udp_sock      => undef,
    multicast_addr    => undef,
    # the following are used by the domain socket code
    ds_rendezvous     => undef, # path to socket, undef if not used
    ds_sun            => undef, # the sockaddr_un for the socket calls
    ds_socket         => undef, # the socket itself; undef == not connected
    ds_fileno         => undef, # file descriptor for the underlying socket
    ds_bufsize        => undef, # maximum dgram size reported by socket
    ds_vec            => undef, # pre-populated vector for the select call
    ds_attempt_ts     => undef, # timestamp when we last tried to connect
  }
}

sub init {
  my $self = shift;

  # multicast-style ?
  if($self->{'config'}->{'log_host_ip'} =~ /^(22[456789]|23\d)\./) {
    $self->{'multicast_addr'} = $self->{'config'}->{'log_host_ip'}.':'.
      $self->{'config'}->{'log_host_port'};
    my $multicast_pkg = 'IO::Socket::Multicast';
    eval "use $multicast_pkg;" unless exists $::{$multicast_pkg.'::'};
    $self->{'log_udp_sock'} = $multicast_pkg->new( Proto => 'udp' );
    unless ($self->{'log_udp_sock'}) {
	    $self->{'server'}->s_log(1,"could not open multicast socket: $!");
	    die "could not open multicast socket: $!";
    }
    $self->{'log_udp_sock'}->mcast_ttl(2);
  }

  # unicast-style ?
  else {
    $self->{'log_host_ip'} = inet_aton($self->{'config'}->{'log_host_ip'});
    $self->{'log_host_portaddr'} = 
      sockaddr_in($self->{'config'}->{'log_host_port'},$self->{'log_host_ip'});
  }

  # is the domain socket enabled?
  if (my $dsr = $self->{'config'}->{'domain_sock'}) {
	  $self->{'ds_rendezvous'} = $dsr;
	  $self->{'ds_sun'} = sockaddr_un($dsr);
  }
}

# turn a hash of data into a log line
#
# format for each field is
#  \tKEY:VALUE
# where VALUE cannot contain any tab characters (either substitute with
#   a literal '\t' or a space if the tabbiness isn't important)
# the record is any number of fields concatenated together, terminated with
#  \t\n
# this format allows for easy parsing by downstream -- split on \t,
# then split each resultant element on the first ':', e.g.
#  my %r = map { split(':',$_,2) } grep { chomp; $_ } split(/\t/, $line);

# we choose the following keys for field names
#
# b bytes transferred
# c session cookie
# h http host
# i remote ip
# m http method
# n mcp node
# o mcp module
# p uri path
# r referer
# t timestamp
# s http status code
# u user agent
# x truncated_p?

# Certain keys are more important than others.  When creating the line
# we walk the following ordered list, so if we need to truncate an
# entry we still have logged the most important data.

my $_field_map = [
                  [ 'timestamp' => 't' ],
                  [ 'local_ip' => 'n' ],
                  [ 'remote_ip' => 'i' ],
                  [ 'served_by' => 'o' ],
                  [ 'status' => 's' ],
                  [ 'content_length' => 'b' ],
                  [ 'session_cookie' => 'c' ],
                  [ 'request_method' => 'm' ],
                  [ 'request_host' => 'h' ],
                  [ 'request_uri' => 'p' ],
                  [ 'user_agent' => 'u' ],
                  [ 'referer_url' => 'r' ],
                 ];

sub tab_serializer($;$) {
	my ($log_hash, $max_length) = @_;

	my @fields;
	my $log_keys_handled = {};
	for my $mapping (@$_field_map) {
		my ($log_hash_key, $tab_key) = @$mapping;
		my $value = $log_hash->{$log_hash_key};
		# don't log empty fields
		next unless defined($value);
		next if $value eq '-';
		# remove tabs
		$value =~ s/\t/ /g if $value;
		# attach some extra bits if available
		if ($tab_key eq 'o') {
			my $h = $log_hash->{'forward_host'};
			if (defined($h) && $h ne '-') {
				$value .= ':' . $h;
				my $p = $log_hash->{'forward_port'};
				$value .= ':' . $p if (defined($p) && $p ne '-');
			}
		} elsif ($tab_key eq 'n') {
			my $p = $log_hash->{'request_port'};
			$value .= ':' . $p if (defined($p) && $p ne '-');
		}
		push (@fields, join(':', $tab_key, $value));
	}

	# assemble the line
	my $line = '';

	# are we performing length checks?
	unless ($max_length) {
		# nope, just assemble it
		$line = ("\tx:0"					  # not truncated
		         . "\t" . join("\t", @fields) # the fields
		        . "\t\n");					  # trailing bits

	} else {
		# checking for length

		my $line_len = 6;			# future initial "\tx:[01]" + "\t\n"
		my $truncated_p = 0;

		for my $f (@fields) {
			my $l = length($f);
			my $new_len = $line_len + $l + 1; # "\t" + field
			if ($new_len > $max_length) {
				my $overflow = $new_len - $max_length;
				$truncated_p = 1;
				$l -= $overflow;
				if ($l >= 3) {
					# if we don't leave three characters (that is, the
					# one-character key, colon, and one character of
					# the value), there's no point in including this
					# field at all
					$f = substr($f, 0, $l);
					$line .= "\t" . $f;
				}
				last;
			}
			$line .= "\t" . $f;
			$line_len = $new_len;
		}

		$line = "\tx:" . $truncated_p . $line . "\t\n";
	}

	return $line;
}

# This code implements a persistent connection to a local domain
# socket.  Because of the structure of the main epolld loop, I can't
# easily insert the domain socket's file descriptor.  Instead, I've
# decided that it's OK to spend a few cycles here trying to write the
# log entry, reasoning that any overall bottleneck will not be in the
# few nanoseconds spent writing (or failing to write) to this here
# domain socket.

# log the response, returning true on a successful write to the socket
sub _ds_log_response($$) {
	use bytes;
	my ($self, $log_hash) = @_;

	# don't log unless this is enabled
	return unless $self->{'ds_rendezvous'};

	my $success = 0;
	my $tries = 3;
	my $payload = undef;

	while ($tries--) {
		# make sure the socket is open
		unless ($self->{'ds_socket'}) {
			# but only try once per second to avoid a thundering herd
			my $now = scalar(time());
			unless ($now - $self->{'ds_attempt_ts'} > 1) {
				print STDERR "_ds_log_response: not yet time to connect\n" if DEBUG;
				return 0;
			}
			$self->{'ds_attempt_ts'} = $now;
			print STDERR "_ds_log_response: connect attempt at $now\n" if DEBUG;
			unless (socket($self->{'ds_socket'}, PF_UNIX, SOCK_DGRAM, 0)) {
				print STDERR "_ds_log_response: socket err: $!\n" if DEBUG;
				close($self->{'ds_socket'});
				$self->{'ds_socket'} = undef;
				return 0;
			}
			# set nonblocking
			my $flags = fcntl($self->{'ds_socket'}, F_GETFL, 0);
			unless ($flags) {
				print STDERR "_ds_log_response: can't get flags: $!\n" if DEBUG;
				close($self->{'ds_socket'});
				$self->{'ds_socket'} = undef;
				return 0;
			}
			unless (fcntl($self->{'ds_socket'}, F_SETFL, $flags | O_NONBLOCK)) {
				print STDERR "_ds_log_response: can't set nonblock: $!\n" if DEBUG;
				close($self->{'ds_socket'});
				$self->{'ds_socket'} = undef;
				return 0;
			}
			unless (connect($self->{'ds_socket'}, $self->{'ds_sun'})) {
				print STDERR "_ds_log_response: connect err: $!\n" if DEBUG;
				close($self->{'ds_socket'});
				$self->{'ds_socket'} = undef;
				return 0;
			}
			$self->{'ds_fileno'} = fileno($self->{'ds_socket'});
			$self->{'ds_vec'} = '';
			vec($self->{'ds_vec'}, $self->{'ds_fileno'}, 1) = 1;

			$self->{'ds_bufsize'} = getsockopt($self->{'ds_socket'}, SOL_SOCKET, SO_SNDBUF);
			$self->{'ds_bufsize'} = unpack("I", $self->{'ds_bufsize'});

			print STDERR "_ds_log_response: ds_bufsize $self->{'ds_bufsize'}\n" if DEBUG;
		}

		unless ($payload) {
			# reformat the log_hash into a string which can be sent over the
			# domain socket
			$payload = tab_serializer($log_hash, $self->{'ds_bufsize'});
		}

		# wait (a very short time) for the socket to be ready
		my ($wout, $eout);
		my ($nfound, $timeleft) = select(undef, $wout = $self->{'ds_vec'}, $eout = $self->{'ds_vec'}, 0.0001);

		# close the socket on error and retry
		if (vec($eout, $self->{'ds_fileno'}, 1)){
			print STDERR "_ds_log_response: ds_socket err, closing\n" if DEBUG;
			close($self->{'ds_socket'});
			$self->{'ds_socket'} = undef;
			next;
		}

		# write to the socket
		if (vec($wout, $self->{'ds_fileno'}, 1)) {
			if (send($self->{'ds_socket'}, $payload, 0, $self->{'ds_sun'})) {
				# FIXME: this doesn't handle partial writes
				$success = 1;
				last;
			} else {
				print STDERR "_ds_log_response: ds_socket send err, closing: $!\n" if DEBUG;
				close($self->{'ds_socket'});
				$self->{'ds_socket'} = undef;
				next;
			}
		}
	}

	return $success;
}

sub event_client_disconnect {
  my($self,$event) = @_;
  my $request = $event->{'request'};
  return undef unless ref $request;
  return undef unless(ref($request) eq 'MCP::Request'); 
  my $status       = $request->{'response_status'};
  return undef unless $status;
  my $client_connection = $request->{'client_connection'};
  my $origin_connection = $request->{'origin_connection'};
  my $local_ip     = $client_connection->{'local_ip'};
  my $request_port = $client_connection->{'local_port'};
  my $content_length = $request->get_response_content_length();
  my $remote_ip    = $client_connection->remote_ip();
  my $request_uri = $request->{'path'};
  my($request_method,$request_path,$request_proto) = $request->parse_rline();
  my $request_host = $request->{'host'};
  my $request_proto = 'HTTP/' . $request->{'proto'};
  my $served_by = $request->{'response_served_by'} ? $request->{'response_served_by'} : '-';
  my $forward_host = 
    $origin_connection? $origin_connection->{'remote_ip'}: '-';
  my $forward_port = 
    $origin_connection? $origin_connection->{'remote_port'}: '-';
  my $forward_host_confidence = '-'; #TODO
  my $forward_retry_num       = '-'; #TODO
  my $timer_request         = $request->get_duration() || 0;
  my $timer_fixup           = $request->{'timers'}->{'fixup'} || 0;
  $timer_fixup -= $request->{'timers'}->{'forward_request'};
  my $timer_response        = $request->{'timers'}->{'response'} || 0;
  my $timer_forward_connect = $request->{'timers'}->{'forward_connect'} || 0;
  my $timer_forward_request = $request->{'timers'}->{'forward_request'} || 0;
  my $timer_cache_fetch     = $request->{'timers'}->{'cache_fetch'} || 0;
  my $timer_send            = $request->{'timers'}->{'send'} || 0;
  my $wants_gzip            = $request->wants_gzip();
  my $x_forwarded_for = $request->get_header_value('X-Forwarded-For') || '-';
  my $referer_url     = $request->get_header_value('Referer') || '-';
  my $user_agent      = $request->get_header_value('User-Agent') || '-';
  my $throttled       = '-'; #TODO
  my $throttle_header = '-'; #TODO
  my $session_cookie        = $request->{'session_cookie_id'} || '-';
  my $format_cookie         = $request->{'format_cookie'} || '-';
  my $forward_pool = $request->{'origin_pool'};
  unless ($forward_pool) {
	  if ($served_by eq 'DirectCache'
	      || $served_by eq 'BlobForwarder'
	      || $served_by eq 'ImageURL'
	      || $served_by eq 'Forwarder' # won't happen, but here for completeness
	     ) {
		  # these three modules result in pages being delivered to the
		  # client; for each of them, we want to know which origin
		  # pool and host would have delivered the page -- ask the
		  # Forwarder module
		  if (my $fm = $self->{'server'}->{'request_modules'}->{'Forwarder'}) {
			  my ($ct, $pool) = $fm->select_origin_server_for_request($request);
			  if ($ct && $pool) {
				  my ($h,$p) = split(/\:/o, $ct);
				  $forward_pool = $pool;
				  # only overwrite the host and port if they're not populated
				  $forward_host ||= $h;
				  $forward_port ||= $p;
			  }
		  }
	  }

	  # nothing matched?  say it was handled internally
	  $forward_pool ||= '-';
  }

  my $log_hash = {
    timestamp               => scalar(time()),
    local_ip                => $local_ip,
    remote_ip               => $remote_ip,
    request_uri             => $request_uri,
    request_method          => $request_method,
    request_host            => $request_host,
    request_port            => $request_port,
    request_proto           => $request_proto,
    status                  => $status,
    content_length          => $content_length,
    served_by               => $served_by,
    forward_host            => $forward_host,
    forward_port            => $forward_port,
    forward_host_confidence => $forward_host_confidence,
    forward_retry_num       => $forward_retry_num,
    forward_pool            => $forward_pool,
    timer_request           => $timer_request,
    timer_fixup             => $timer_fixup,
    timer_response          => $timer_response,
    timer_forward_connect   => $timer_forward_connect,
    timer_forward_request   => $timer_forward_request,
    timer_cache_fetch       => $timer_cache_fetch,
    timer_send              => $timer_send,
    wants_gzip              => $wants_gzip,
    x_forwarded_for         => $x_forwarded_for,
    throttled               => $throttled,
    throttle_header         => $throttle_header,
    session_cookie          => $session_cookie,
    format_cookie           => $format_cookie,
    referer_url             => $referer_url,
    user_agent              => $user_agent,
  };
  # FIXME: count logging errors and report periodically, being careful
  # not to cause syslogspew
  $self->_ds_log_response($log_hash);
  $self->_udp_log_response($log_hash);
  $self->{'server'}->s_log(4,$request->{'id'}." done logging request");
}

sub _udp_log_response {
  use bytes;
  my($self,$message) = @_;

  my $udp_sock;
  unless($udp_sock = $self->{'log_udp_sock'}) {
    if($self->{'multicast_addr'}) {
      my $multicast_pkg = 'IO::Socket::Multicast';
      eval "use $multicast_pkg;" unless exists $::{$multicast_pkg.'::'};
      $udp_sock = $multicast_pkg->new( Proto => 'udp' );
      unless ($udp_sock) {
	      $self->{'server'}->s_log(1,"could not open multicast socket: $!");
	      die "could not open multicast socket: $!";
      }
    }
    else {
      socket($udp_sock, PF_INET, SOCK_DGRAM, getprotobyname("udp"));
      setsockopt($udp_sock, SOL_SOCKET, SO_BROADCAST, pack("l", 1));
    }
    $self->{'log_udp_sock'} = $udp_sock;
  }
  $message = $self->simple_serializer($message,[LOG_KEYS],LOG_FORMAT_VERSION);
  if(length($message) > MAX_UDP_DGRAM_SIZE) {
      $message = substr($message,0,MAX_UDP_DGRAM_SIZE - 6);
      $message .= '%BORK%';
      # FIXME: there is no check here to make sure that
      # all of the other fields fit into the packet
  }
  if($self->{'multicast_addr'}) {
    $udp_sock->mcast_send($message, $self->{'multicast_addr'});
  }
  else {
    send($udp_sock,$message,0,$self->{'log_host_portaddr'}) == length($message)
      or $self->{'server'}->s_log(1,"cannot send to log udp: $!");
  }
}

# take a hash and emit it as [ key1 \n value1 \n .. keyn \n valuen ]
# making sure to put request_uri at the end so that's the thing we'll
# truncate later...
sub simple_serializer {
  my($caller,$hashref,$logkeys,$logfmt_version) = @_;
  my $scalar = '';
  my @v;
  foreach my $hkey (@{$logkeys}) {
    push @v, $hashref->{$hkey};
  }
  return join("\n",$logfmt_version,@v);
}



1;
__END__


