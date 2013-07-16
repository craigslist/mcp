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

package MCP::RequestModule::HeaderStamper;
use base 'MCP::RequestModule';
use strict;


sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    modify_request_headers => {},
    modify_response_headers => {},
  }
}

sub init {
  my $self = shift;
  my $config = $self->{'server'}->{'config'};

  $self->{'modify_request_headers'} = 
    $config->{'header_stamper'}->{'modify_request_headers'};
  $self->{'modify_response_headers'} = 
    $config->{'header_stamper'}->{'modify_response_headers'};
}


sub event_origin_connect {
  my($self,$event) = @_;
  my $request = $event->{'request'};
  my $client_connection = $request->{'client_connection'};

  # no Keep-Alive, thanks
  $request->set_header_value('Keep-Alive', undef),
  $request->set_header_value('Connection', 'close');

  # If-Modified-Since needn't be passed through to origin server
  $request->set_header_value('If-Modified-Since', undef);

  # monkey with X-Forwarded-For
  my $remote_ip = $client_connection->remote_ip();
  my $xff = $request->get_header_value('X-Forwarded-For');
  $xff = join(',', @{$xff}) if(ref $xff);
  $request->set_header_value('X-Forwarded-For-From-Client', $xff);
  $request->set_header_value('X-Forwarded-For', $remote_ip);

  # be explicit about the gzip thing, if necessary
  my $accept_encoding = $request->get_header_value('Accept-Encoding');
  if(!$accept_encoding && $request->wants_gzip()) {
    $request->set_header_value('Accept-Encoding', 'gzip');
  }

  # apply any configured host_path-match header modifications
  my $host_path = $request->get_host_path();
  foreach my $mod (@{$self->{'modify_request_headers'}}) {
    my($match_re,$changes) = @{$mod};
    next unless($host_path =~ /$match_re/);
    foreach my $header (keys %{$changes}) {
      $request->set_header_value($header,undef);
      $request->set_header_value($header,$changes->{$header});
    }
    last;
  }
}

sub event_received_client_headers {
  my($self,$event) = @_;
  my $config = $self->{'server'}->{'config'}->{'header_stamper'};
  my $request = $event->{'request'};

  # look for remote ip header if we're configured to do so
  if(my $remote_ip_header = $config->{'remote_ip_header'}) {
    if(my $header_val = $request->get_header_value($remote_ip_header)) {
      if(my $cc = $request->{'client_connection'}) {
        $cc->{'remote_ip'} = $header_val;
      }
    }
  }

  #XXX temporary: keep an eye on strange Range: headers
  if(my $range = $request->get_header_value('Range')) {
    if(length($range) > 64) {
      my $cc = $request->{'client_connection'};
      my $cip = ref($cc)? $cc->{'remote_ip'}: '-';
      $self->{'server'}->s_log(1,"zany Range header ($cip): $range");
    }
  }
}

sub event_received_origin_headers {
  my($self,$event) = @_;
  my $request = $event->{'request'};

  # no Keep-Alive, thanks
  $request->set_response_header_value('Keep-Alive', undef),
  $request->set_response_header_value('Connection', 'close');

  # no Etags, k plz thx
  $request->set_response_header_value('Etag', undef),

  # apply any configured host_path-match header modifications
  my $host_path = $request->get_host_path();
  foreach my $mod (@{$self->{'modify_response_headers'}}) {
    my($match_re,$changes) = @{$mod};
    next unless($host_path =~ /$match_re/);
    foreach my $header (keys %{$changes}) {
      $request->set_response_header_value($header,undef);
      $request->set_response_header_value($header,$changes->{$header});
    }
    last;
  }
}

sub get_status_page {
  my($self,$request) = @_;
  my $config = $self->{'server'}->{'config'}->{'header_stamper'};
  
  my $status = [];
      
  my $req_config_table = {
    type  => 'table',
    title => 'Request Header Configuration',
    data  => [['Match Regular Expression:','Header:','New Value:']],
  };
  foreach my $item (@{$config->{'modify_request_headers'}}) {
    my($match_re,$header_mods) = @{$item};
    push(@{$req_config_table->{'data'}}, [ $match_re, undef, undef ]);
    foreach my $header (sort keys %{$header_mods}) {
      push(@{$req_config_table->{'data'}}, 
        [ undef, $header, $header_mods->{$header} ]);
    }
  }  
  push(@{$status}, $req_config_table);
  
  my $res_config_table = {
    type  => 'table',
    title => 'Response Header Configuration',
    data  => [['Match Regular Expression:','Header:','New Value:']],
  };
  foreach my $item (@{$config->{'modify_response_headers'}}) {
    my($match_re,$header_mods) = @{$item};
    push(@{$res_config_table->{'data'}}, [ $match_re, undef, undef ]);
    foreach my $header (sort keys %{$header_mods}) {
      push(@{$res_config_table->{'data'}}, 
        [ undef, $header, $header_mods->{$header} ]);
    }
  }  
  push(@{$status}, $res_config_table);
  
  return $status;
}



1;
__END__


