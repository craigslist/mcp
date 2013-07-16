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

package MCP::RequestModule::Exceptions;
use base 'MCP::RequestModule';
use strict;

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    config => $args->{'server'}->{'config'}->{'exceptions'},
  };
}

sub init { }

# Point of interest: every one of these events are generated in
# Poller/Connection/Client.pm.

sub event_client_idle_timeout {
  my($self,$event) = @_;
  my $config = $self->{'config'};
  my $request = $event->{'request'};
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{'client_idle_timeout'});
  return -1;
}

sub event_failed_to_parse_request { 
  my($self,$event) = @_;
  my $config = $self->{'config'};
  my $request = $event->{'request'};
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{'failed_to_parse_request'});
  return -1;
}

sub event_request_uri_too_long { 
  my($self,$event) = @_;
  my $config = $self->{'config'};
  my $request = $event->{'request'};
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{'request_uri_too_long'});
  return -1;
}

sub event_failed_to_parse_response { 
  my($self,$event) = @_;
  my $config = $self->{'config'};
  my $request = $event->{'request'};
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{'failed_to_parse_response'});
  return -1;
}

sub event_admin_port_required { 
  my($self,$event) = @_;
  my $config = $self->{'config'};
  my $request = $event->{'request'};
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{'admin_port_required'});
  return -1;
}

sub event_request_url_not_modified {
  my($self,$event) = @_;
  my $config = $self->{'config'};
  my $request = $event->{'request'};
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{'request_url_not_modified'});
  return -1;
}

sub event_max_request_length_exceeded {
  my($self,$event) = @_;
  my $config = $self->{'config'};
  my $request = $event->{'request'};
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{'max_request_length_exceeded'});
  return -1;
}


1;
__END__


