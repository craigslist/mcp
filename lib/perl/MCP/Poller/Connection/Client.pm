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

package MCP::Poller::Connection::Client;
use base 'MCP::Poller::Connection::HTTP';
use MCP::Request;
use IO::Socket::INET;
use Socket qw(IPPROTO_TCP TCP_NODELAY PF_INET SOCK_STREAM SO_REUSEADDR SOL_SOCKET);
use Fcntl qw(F_GETFL F_SETFL O_NONBLOCK);
use POSIX;
use strict;

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    is_listener        => 0,
    listener           => $args->{'listener'},
    request            => undef,
    local_ip           => $args->{'local_ip'},
    local_port         => $args->{'local_port'},
    remote_ip          => undef,
    is_default_port    => ($args->{'is_default_port'}? 1: 0),
    virt_port          => $args->{'virt_port'},
    is_admin           => $args->{'is_admin'},
    timeout_time       => undef,
  } 
}


sub init {
    my($self,$args) = @_;
    $self->SUPER::init($args);
    my $server = $self->{'poller'}->{'server'};

    # if a socket was passed, assume this is not the listener
    if($args->{'socket'}) {
        my $peername = getpeername($args->{'socket'});
        my($port, $iaddr) = $peername? sockaddr_in($peername): (undef,undef);
        $self->{'remote_ip'} = $iaddr? inet_ntoa($iaddr): '?';

        $self->watch_read();
   
        return;
    }

    # if listener config passed in, assume this is a listener
    if(my $listener_config = $args->{'listener_config'}) {
        $self->{'is_listener'} = 1;
        $self->{'is_default_port'} = $listener_config->{'is_default'}? 1: 0;
        $self->{'virt_port'}       = $listener_config->{'virt_port'};
        $self->{'is_admin'}        = $listener_config->{'is_admin'}? 1: 0;

        for(;;) { # we'll keep trying to start a listener forever
            # create the listener socket, watch it for read
            unless($self->{'listener'}) {
                $self->{'listener'} = 
                    $self->create_listener($listener_config,$server);
            }
            next unless $self->{'listener'};
        
            $self->{'socket'} = $self->{'listener'};
            $self->{'local_ip'} = $listener_config->{'ip'};
            $self->{'local_port'} = $listener_config->{'port'};
            $self->watch_read();
            $server->s_log(1, 'waiting to accept on '.
                $listener_config->{'ip'}.':'.$listener_config->{'port'});
            last;
        }
    }
}

sub create_listener {
    my($caller,$listener_config) = @_;

    my $listener = undef;
    socket($listener, PF_INET, SOCK_STREAM, IPPROTO_TCP);
    setsockopt($listener, SOL_SOCKET, SO_REUSEADDR, pack("l", 1));
    my $sockflags = fcntl($listener, F_GETFL, 0);
    fcntl($listener, F_SETFL, $sockflags | O_NONBLOCK);
    bind($listener, Socket::sockaddr_in($listener_config->{'port'}, Socket::inet_aton($listener_config->{'ip'})));
    unless(listen($listener, $listener_config->{'queue_size'})) {
        print STDERR 'failed to start listener on '.
            $listener_config->{'ip'}.':'.$listener_config->{'port'}.
            '...retrying' ;
        sleep 5;
        return undef;
    }

    return $listener;
}

sub event_ready_for_read {
  my $self = shift;
  my $config = $self->{'poller'}->{'server'}->{'config'};

  # if this is a listener socket, accept(), create request object
  if($self->{'is_listener'}) {
    my $new_socket;
    my $rv = accept($new_socket, $self->{'socket'});
    if(!defined($rv) && ($!{EAGAIN} || $!{EWOULDBLOCK})) {
        return;
    }

    my $new_connection = $self->new({
      'socket'          => $new_socket,
      'poller'          => $self->{'poller'},
      'listener'        => $self->{'listener'},
      'local_ip'        => $self->{'local_ip'},
      'local_port'      => $self->{'local_port'},
      'is_default_port' => $self->{'is_default_port'},
      'virt_port'       => $self->{'virt_port'},
      'is_admin'        => $self->{'is_admin'},
    });
    my $new_request = MCP::Request->new({
      'client_connection' => $new_connection,
      'request_config'    => $config->{'requests'},
    });
    $new_connection->{'request'} = $new_request;
    $new_connection->reset_idle_timeout();

	######################################################################
	# NOTE: as yet unused, but could be useful in the future
	######################################################################
    # $self->send_request_event({
    #   type    => 'client_connect',
    #   request => $new_request,
    # });
  }

  # otherwise, do the usual stuff
  else {
    $self->reset_idle_timeout();
    $self->SUPER::event_ready_for_read(@_);
    $self->check_request_length();
  }
}

sub reset_idle_timeout {
  my $self = shift;

  my $idle_timeout = 
    $self->{'poller'}->{'server'}->{'config'}->{'requests'}->{'idle_timeout'};

  $self->{'timeout_time'} = scalar(time()) + $idle_timeout;
}

sub cancel_idle_timeout {
  my $self = shift;

  $self->{'timeout_time'} = undef;
}

sub check_request_length {
  my $self = shift;

  # make sure we have not exceeded request length limit
  my $max_length = 
    $self->{'poller'}->{'server'}->{'config'}->{'requests'}->{'max_length'};
  if($self->{'bytes_read'} > $max_length) {
    $self->send_request_event({
      type    => 'max_request_length_exceeded',
      request => $self->{'request'},
    });
  }
}

sub poll_tick_1s {
  my($self,$now) = @_;

  if(my $request = $self->{'request'}) {
      my $headers_timeout = $self->{'poller'}->{'server'}->{'config'}->{'requests'}->{'headers_timeout'};
      my $request_timeout = $self->{'poller'}->{'server'}->{'config'}->{'requests'}->{'request_timeout'};
      my $duration = $request->get_duration();
      unless($self->{'received_headers'}) {
          if($duration >= $headers_timeout) {
              $self->send_request_event({
                type    => 'client_idle_timeout',
                request => $self->{'request'},
              });
          }
      }
      if($duration >= $request_timeout) {
          $self->send_request_event({
            type    => 'client_idle_timeout',
            request => $self->{'request'},
          });
      }
  }


  return undef unless $self->{'timeout_time'};
  if($now >= $self->{'timeout_time'}) {
    $self->send_request_event({
      type    => 'client_idle_timeout',
      request => $self->{'request'},
    });
  }

}

sub event_disconnect {
  my $self = shift;
 
  $self->close();
} 

sub event_received_rline {
  my $self = shift;
  $self->SUPER::event_received_rline();

  # everything relies on a parsed rline, so do that here so we can
  # fail early if something invalid has been passed in

  # note that rline here is the REQUEST from a client

  # if someone has connected and not sent any rline (e.g the 'ring and
  # run' case), don't fire any more events
  return if (! defined $self->{'request'}->{'client_connection'}->{'raw_rline'}
	     || $self->{'request'}->{'client_connection'}->{'raw_rline'} eq '');

  #print STDERR "got rline '" . $self->{'request'}->{'client_connection'}->{'raw_rline'} . "'\n";

  # try to parse the rline
  my ($method, $path, $proto) = $self->{'request'}->parse_rline();
  my $server = $self->{'request'}->{'client_connection'}->{'poller'}->{'server'};
  unless($method && $path && $proto) {
    # fail to parse rline
    $server->send_request_event({
      type    => 'failed_to_parse_request',
      request => $self->{'request'},
    });
  } else {
    if(length($path) > 8000) {
      $server->send_request_event({
        type    => 'request_uri_too_long',
        request => $self->{'request'},
      });
    }
    else {
      # success parsing rline
      $self->send_request_event({
        type    => 'received_client_rline',
        request => $self->{'request'},
      });
    }
  }
}

sub event_received_headers {
  my $self = shift;
  $self->SUPER::event_received_headers();

  my $config = $self->{'poller'}->{'server'}->{'config'};

  # mark request as buffered if Content-Length is larger than our max
  my $max_unbuffered_request_length = 
    $self->{'poller'}->{'server'}->{'config'}->{'requests'}->{'max_unbuffered_length'};
  my $header_request_length =
    $self->{'request'}->get_header_value('Content-Length') || 0;
  if($header_request_length > $max_unbuffered_request_length) {
    $self->{'request'}->{'buffer_content_data'} = 
      ($header_request_length - $max_unbuffered_request_length);
  }

  # since most of the request modules want to examine the headers of a
  # request, we may as well parse them now
  $self->{'request'}->parse_request_headers();

  # dump facility for debugging: this will write out raw request data
  # to a tmp file if 'debug_dump_url' config parameter is set.  this
  # should not be used on a regular basis as it could impact server
  # performance.
  if(my $dumpurl = $config->{'requests'}->{'debug_dump_url'}) {
      my $hostpath = $self->{'request'}->get_host_path();
      if($hostpath =~ /$dumpurl/) {
          open(DFH,">>/tmp/mcp-debug-dump.out");
          print DFH join("\r\n", 
              $self->{'raw_rline'},
              $self->{'raw_headers'},
              $self->{'raw_content'},
              '--',''
          );
          close(DFH);
      }
  }

  $self->send_request_event({
    type    => 'received_client_headers',
    request => $self->{'request'},
  });
}

sub event_content_data_in {
  my $self = shift;
  $self->SUPER::event_content_data_in();

  $self->send_request_event({
    type    => 'received_client_data_chunk',
    request => $self->{'request'},
  });
}

sub close {
  my $self = shift;

  if($self->{'is_listener'}) {
    $self->{'poller'}->{'server'}->s_log(2,
      'closing listener '.$self->{'local_ip'}.':'.$self->{'local_port'});
#XXX sock
    close($self->{'socket'});
#    return POSIX::close($self->{'socket'});
  }
  else {
    return $self->SUPER::close();
  }
}

sub _close {
  my $self = shift;

  $self->cancel_idle_timeout();

  # be sure any associated origin connection is closed
  if(ref($self->{'request'}) eq 'MCP::Request') {
    if(my $origin_connection = $self->{'request'}->{'origin_connection'}) {
      $origin_connection->close();
    }
  }

  # mark this request completed
  $self->{'request'}->mark_completed()
    if ref $self->{'request'};

  $self->SUPER::_close(@_);

  # notify interested parties
  $self->send_request_event({
    type    => 'client_disconnect',
    request => $self->{'request'},
  }) if ref $self->{'request'};

  # this connection is over, forget about request object
  $self->{'request'} = undef;
}

sub remote_ip { shift->{'remote_ip'} }

sub DESTROY {
  my $self = shift;

  $self->{'request'} = undef;
} 




1;
__END__

