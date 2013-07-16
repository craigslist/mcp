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

package MCP::Request;
use base 'MCP::Object';
use Data::Dumper;
use Time::HiRes qw/ gettimeofday /;
use strict;


use constant MAX_REQUEST_ID => 2**31;


my $Last_Request_ID = 0;
sub get_next_request_id { 
  $Last_Request_ID = 0 if($Last_Request_ID > MAX_REQUEST_ID);
  return ++$Last_Request_ID; 
}
sub get_last_id { return $Last_Request_ID; }

my $Active_Requests = {};
my $Num_Served_Requests = 0;
sub get_active_requests { return $Active_Requests; }
sub num_active_requests { return scalar(keys(%{$Active_Requests})); }
sub num_served_requests { return $Num_Served_Requests; } 
sub mark_active { 
  my($self) = @_;
  $Active_Requests->{$self->{'id'}} = $self; #XXX make sure this ref is ok
  $self->{'start_time'} = gettimeofday();
}
sub mark_completed {
  my($self) = @_;
  delete $Active_Requests->{$self->{'id'}};
  $Num_Served_Requests++;
  my $now = gettimeofday();
}
sub get_duration {
  my($self) = @_;
  my $now = gettimeofday();
  return ($now - $self->{'start_time'});
}

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    'request_config'            => $args->{'request_config'},
    'id'                        => $caller->get_next_request_id(),
    'client_connection'         => $args->{'client_connection'},
    'origin_connection'         => undef,
    'origin_pool'               => undef,
    'method'                    => undef,
    'proto'                     => undef,
    'host'                      => undef,
    'path'                      => undef,
    'host_path'                 => undef,
    'header_values'             => undef,
    'header_case'               => {},
    'response_header_values'    => undef,
    'response_header_case'      => {},
    'headers_modified'          => 0,
    'response_headers_modified' => 0,
    'timers'                    => {},
    'stale_cache_hit'           => undef,
    'response_proto'            => undef,
    'response_status'           => undef,
    'response_status_text'      => undef,
    'response_content_length'   => undef,
    'response_served_by'        => undef,
    'purge_content'             => '',
    'purge_content_length'      => undef,
    'purge_index_keys'          => {},
    'purged_pages'              => 0,
    'unpurged_pages'            => 0,
    'sent_response_data'        => 0,
    'start_time'                => 0,
    'last_request_event'        => undef,
    'buffer_content_data'       => 0,
    'origin_connect_pending'    => 0,
    'session_cookie_id'         => undef,
    'format_cookie'             => undef,
    'extra_response_headers'    => {},
    'alternate_forwarder_pool'  => undef,
    'origin_max_retries_event'  => undef,
    'cache_host_path'           => undef,
  }
}

sub init {
  my $self = shift;

  $self->mark_active();
}

sub get_host_path {
  my($self,$useport) = @_;

  # only bother scanning rline and headers once, mmkay?
  return $self->{'host_path'} 
    if(defined $self->{'host_path'});

  # use default host in case we can't match one
  $self->{'host'} = $self->{'request_config'}->{'default_host'};

  my $header_host = $self->get_header_value('Host');
  $self->{'host'} = $header_host 
    if defined $header_host;
  $self->{'host'} = lc($self->{'host'}); # case-insensitive hostnames
  $self->{'host'} =~ s/\.$//;            # ignore trailing '.' in hostname

  my($method,$path,$proto) = $self->parse_rline();
  my $port = $self->{'client_connection'}->{'local_port'};
  if($useport) {
    ($port) = ($self->{'host'} =~ /(:.*)$/);
    $port = '' unless($port);
  } else {
    $self->{'host'} =~ s/:.*$//;           # ignore chars after ':' in hostname
    $port = $self->{'client_connection'}->{'virt_port'}
      if $self->{'client_connection'}->{'virt_port'};
    $port = ($port eq $self->{'request_config'}->{'default_port'})? '': ":$port";
    $port = '' 
      if $self->{'client_connection'}->{'is_default_port'};
  }

  return $self->{'host_path'} = $self->{'host'}.$port.$path;
}

sub parse_rline {
  my $self = shift;

  # only parse rline once for extra speed
  return($self->{'method'},$self->{'path'},$self->{'proto'})
    if(defined $self->{'method'} && defined $self->{'path'});

  # pull method, path, proto from raw rline
  my $methods = $self->{'request_config'}->{'acceptable_methods'};
  my($method,$path,$proto) = ($self->{'client_connection'}->{'raw_rline'} =~
    /^\s*($methods)\s+(\S+)(?:\s+HTTP\/(\d\.\d))?\s*$/io);
  $method = uc($method);
  $path =~ s/^\s*//o; $path =~ s/\s*$//o;
  ($path) = ($path =~ /^http\:\/\/.*?(\/.*)$/io)
    if($path =~ /^http\:\/\//io);
  # per HTTP/1.0 spec, if the version specifier is missing,
  # proto is assumed to be 0.9
  $proto ||= '0.9';

  # remember these values
  $self->{'method'} = $method;
  $self->{'path'}   = $path;
  $self->{'proto'}  = $proto;

  return ($method,$path,$proto);
}

sub get_rline {
  my $self = shift;

  if($self->{'method'}) {
    return join(' ', 
      $self->{'method'}, 
      $self->{'path'}, 
      'HTTP/'.$self->{'proto'}
    );
  }
  else {
    my $client_connection = $self->{'client_connection'};
    return $client_connection->{'raw_rline'};
  }
}

sub parse_response_rline {
  my $self = shift;

  # pull proto, status, status_text from raw rline
  my($proto,$status,$status_text) = 
    ($self->{'origin_connection'}->{'raw_rline'} =~ 
      /^\s*HTTP\/(\d\.\d)\s+(\d+)\s+(.*?)$/io);

  # remember these values
  $self->{'response_proto'}       = $proto;
  $self->{'response_status'}      = $status;
  $self->{'response_status_text'} = $status_text;

  return ($proto,$status,$status_text);
}

sub get_response_status {
  my $self = shift;

  $self->parse_response_rline()
    unless($self->{'response_status'});

  return $self->{'response_status'};
}

sub get_response_rline {
  my $self = shift;

  if($self->{'response_status'}) {
    return join(' ',
      'HTTP/'.($self->{'response_proto'} || $self->{'proto'}),
      $self->{'response_status'},
      $self->{'response_status_text'}
    );
  }
  else {
    my $origin_connection = $self->{'origin_connection'};
    return undef unless ref $origin_connection;
    return $origin_connection->{'raw_rline'};
  }
}

sub get_response_content_length {
  use bytes;
  my $self = shift;
  my $origin_connection = $self->{'origin_connection'};

  # only parse once
  return $self->{'response_content_length'} 
    if defined $self->{'response_content_length'};

  # hits served not from an origin connection MUST set content length on
  # this (request) object, so this return undef does not happen
  return undef unless ref $origin_connection;

  $self->{'response_content_length'} = 
    $self->get_response_header_value('Content-Length') || 
    length($origin_connection->{'raw_content'});

  return $self->{'response_content_length'};
}



# routines for parsing headers for request/response
sub parse_request_headers { shift->_parse_headers(0,@_); }
sub parse_response_headers { shift->_parse_headers(1,@_); }
sub _parse_headers {
  my($self,$from_response) = @_;
  my $values_key = $from_response? 'response_header': 'header';
  my $connection_key = $from_response? 'origin_connection': 'client_connection';

  my $headers = $self->{$connection_key}->{'raw_headers'};
  $headers =~ s/\n\s+/ /sgo;
  $headers =~ s/\r//sgo;

  my $header_hash = {};
  my $header_case_hash = {};
  my @k_v_pairs = ($headers =~ /^(.*?)\:\h*(.*?)$/gmo);
  while(my $key = shift(@k_v_pairs)) {
    my $lckey = lc($key);
    my $val = shift(@k_v_pairs);
    if(exists $header_hash->{$lckey}) {
      my $cur_val = ref $header_hash->{$lckey}? 
        $header_hash->{$lckey}: [$header_hash->{$lckey}];
      push(@{$cur_val}, $val);
      $header_hash->{$lckey} = $cur_val;
      $header_case_hash->{$lckey} = $key;
    }
    else {
      $header_hash->{$lckey} = $val;
      $header_case_hash->{$lckey} = $key;
    }
  }

  # Some edge devices, like load balancers on our network, will insert
  # X-Forwarded-For headers.  We trust some of these devices, and want
  # to treat the IP listed at the end of that xff as the real external
  # client IP.

  my $cc = $self->{'client_connection'};
  my $server = $cc->{'poller'}->{'server'};
  my $tlf = $server->{'config'}->{'requests'}->{'trusted_local_forwarders'};

  # is the connection from a trusted local forwarder?
  if(exists $tlf->{$cc->{'remote_ip'}}
     && exists $header_hash->{'x-forwarded-for'}
     && !ref $header_hash->{'x-forwarded-for'}) {
	  # grab the last element in the comma-delimited list
	  my $last_elem = (split(/\,/o, $header_hash->{'x-forwarded-for'}))[-1];
	  # trim whitespace
	  $last_elem =~ s/^\s+//so; $last_elem =~ s/\s+$//so;
	  # make sure it's ipv4
	  if ($last_elem =~ /^\d+\.\d+\.\d+\.\d+$/o) {
		  $cc->{'remote_ip'} = $last_elem;
	  }
    }

  $self->{$values_key.'_values'} = $header_hash;
  $self->{$values_key.'_case'}   = $header_case_hash;
  return $header_hash;
}

# routines for getting header values for request/response
sub get_header_value { shift->_get_header_value(@_,0); }
sub get_response_header_value { shift->_get_header_value(@_,1); }
sub _get_header_value {
  my($self,$header,$for_response) = @_;
  my $header_hash = $for_response?
    $self->{'response_header_values'}: $self->{'header_values'};
  my $parse_sub = $for_response? 
    'parse_response_headers': 'parse_request_headers';

  # parse the headers from the raw input unless we already did
  $header_hash = $self->$parse_sub()
    unless ref $header_hash;

  return $header_hash->{lc($header)};
}

# routines for setting header values for request/response
sub set_header_value { shift->_set_header_value(@_,0); }
sub set_response_header_value { shift->_set_header_value(@_,1); }
sub _set_header_value {
  my($self,$header,$value,$for_response) = @_;
  
  # grab the appropriate hash, causing a header-parse if needed
  my $header_hash = 
    $for_response? $self->{'response_header_values'}: $self->{'header_values'};
  $header_hash = $self->_parse_headers($for_response)
    unless ref $header_hash;
  my $header_case_hash = 
    $for_response? $self->{'response_header_case'}: $self->{'header_case'};

  # mark that we've modified the headers
  my $modified_key = 
    $for_response? 'response_headers_modified': 'headers_modified';
  $self->{$modified_key} = 1; 

  # change the value
  if(defined $value) {
    $header_hash->{lc($header)} = $value;
    $header_case_hash->{lc($header)} = $header;
  }
  else {
    delete $header_hash->{lc($header)};
    delete $header_case_hash->{lc($header)};
  }
}

# routines for fetching stringy headers for response or request
sub get_header_string {
  my $self = shift;

  # if we have not altered any headers since receiving them, return raw data
  return $self->{'client_connection'}->{'raw_headers'}
    unless $self->{'headers_modified'};

  # otherwise, generate a new header string
  return $self->_generate_header_string(0);
}
sub get_response_header_string {
  my($self,$for_cache) = @_;

  # if we have not altered any headers since receiving them, return raw data
  return $self->{'origin_connection'}->{'raw_headers'}
    unless $self->{'response_headers_modified'};

  # otherwise, generate a new header string
  return $self->_generate_header_string(1,$for_cache);
}
sub _generate_header_string {
  my($self,$for_response,$for_cache) = @_;

  # grab the appropriate hash (either request or response or passed hash)
  my $header_hash = 
    $for_response? $self->{'response_header_values'}: $self->{'header_values'};
  $header_hash = $for_response
    if ref $for_response;
  my $header_case_hash = 
    $for_response? $self->{'response_header_case'}: $self->{'header_case'};
  $header_case_hash = { map { $_ => $_ } keys(%{$header_hash}) }
    if ref $for_response;

  # tack on extra headers for response case
  if(!$for_cache && $for_response) {
      map {
          $header_hash->{$_} = $self->{'extra_response_headers'}->{$_};
          $header_case_hash->{$_} = $_;
      } keys(%{$self->{'extra_response_headers'}});
  }

  # carefully join it all together again
  my $headers = [];
  foreach my $header (keys %{$header_hash}) {
    my $values = $header_hash->{$header};
    $values = [ $values ]
      unless ref $values;
    foreach my $value (@{$values}) {
      my $cased_header = $header_case_hash->{$header};
      push(@{$headers}, $cased_header.': '.$value);
    }
  }

  return join("\r\n", @{$headers});
}

sub wants_gzip {
  my $self = shift;

  my $accept_encoding = $self->get_header_value('Accept-Encoding');
  my($method,$path,$proto) = $self->parse_rline();
 
  # if Accept-Encoding allows gzip or if Accept-Encoding is blank on an
  # HTTP/1.1 request, assume compression wanted
  return 1 if($accept_encoding =~ /gzip/o);
# (not safe?)  return 1 if(($proto eq '1.1') && !defined $accept_encoding);

  return 0;
}

sub response_compressed {
  my $self = shift;

  my $content_encoding = $self->get_response_header_value('Content-Encoding');
  return 1 if($content_encoding =~ /gzip/o);

  return 0;
}

sub remote_ips {
  my $self = shift;
  return undef unless ref $self;
  my $client_connection = $self->{'client_connection'};
  return undef unless ref $client_connection;

  my @ips = ( $client_connection->remote_ip() );
  my $xff = $self->get_header_value('X-Orig-Forwarded-For');
  return @ips unless $xff;
  foreach my $xff_ip (split(/\,/o,$xff)) {
    $xff_ip =~ s/^\s+//so; $xff_ip =~ s/\s+$//so;
    next unless($xff_ip =~ /^\d+\.\d+\.\d+\.\d+$/o);
    push @ips, $xff_ip;
  }

  return @ips;
}

sub send_response {
  use bytes;
  my($self,$response) = @_;
  my $client_connection = $self->{'client_connection'};
  my $proto = $self->{'proto'} || '1.0';

  # if content was passed, set Content-Length header
  if(defined $response->{'content'}) {
    $response->{'headers'} ||= {};
    $response->{'headers'}->{'Content-Length'} = length($response->{'content'});
    $response->{'content_length'} = length($response->{'content'});
  }

  # generate header string if header ref was passed
  my $header_string = ref $response->{'headers'}?
    $self->_generate_header_string($response->{'headers'}): '';

  # write data out the client socketr: rline, headers, and content
  $client_connection->write_data(
    join(' ',"HTTP/$proto",$response->{'status'},$response->{'status_text'}).
    "\r\n".
    $header_string."\r\n\r\n".
    (defined $response->{'content'}? $response->{'content'}: '')
  );

  # register response for logging and close connection
  $self->register_response($response);
  $client_connection->close();
}

sub register_response {
  my($self,$response) = @_;

  my $callpkg = (caller(0))[0];
  $callpkg = (caller(1))[0] unless $callpkg =~ /RequestModule/;
  $callpkg =~ s/^MCP::RequestModule:://;

  $self->{'response_status'}         = $response->{'status'};
  $self->{'response_status_text'}    = $response->{'status_text'};
  $self->{'response_content_length'} = $response->{'content_length'};
  $self->{'response_served_by'}      = $callpkg;

}

1;
__END__

