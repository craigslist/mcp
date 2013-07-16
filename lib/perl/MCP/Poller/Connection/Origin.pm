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

package MCP::Poller::Connection::Origin;
use base 'MCP::Poller::Connection::HTTP';
use Socket qw(PF_INET IPPROTO_TCP SOCK_STREAM);
use POSIX;
use strict;


sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    connect_to        => $args->{'connect_to'},
    pool              => $args->{'pool'},
    pending_connect   => 0,
    connected         => 0,
    request           => $args->{'request'},
    sending_request   => 0,
    remote_ip         => undef,
    remote_port       => undef,
    content_length    => 0,
  }
}

sub init {
  my($self, $args) = @_;

  my($host,$port) = split(/\:/o, $self->{'connect_to'});
  $self->{'remote_ip'}   = $host;
  $self->{'remote_port'} = $port;

  # build a socket, start the connect
  my $socket;
  socket $socket, PF_INET, SOCK_STREAM, IPPROTO_TCP;
  unless($socket && defined fileno($socket)) {
    $self->{'poller'}->{'server'}->s_log(1,"ORIGIN SOCKET CREATION ERROR: $!");
    return undef;
  }
  IO::Handle::blocking($socket, 0);
  connect $socket, Socket::sockaddr_in($port, Socket::inet_aton($host));
  $self->{'socket'} = $socket;
  
  # watch socket for writability
  $self->{'pending_connect'} = scalar(time());
  $self->watch_write();

  # register this as the origin connection for this request
  $self->{'request'}->{'origin_connection'} = $self;

  ######################################################################
  # NOTE: as yet unused, but could be useful in the future
  ######################################################################
  # notify that we're waiting for a connect
  # $self->send_request_event({
  #   type    => 'pending_origin_connect',
  #   request => $self->{'request'},
  # });
}

sub event_ready_for_write {
  my $self = shift;

  if($self->{'pending_connect'}) {
    $self->{'pending_connect'} = 0;
    $self->{'connected'} = 1;
    $self->unwatch_write();

    $self->send_request_event({
      type    => 'origin_connect',
      request => $self->{'request'},
    });
  }

  else {
    return $self->SUPER::event_ready_for_write(@_);
  }
}

sub event_hangup { shift->event_connect_fail() }

sub event_connect_fail {
  my $self = shift;

  $self->send_request_event({
    type    => 'origin_connect_fail',
    request => $self->{'request'},
  });
}

sub event_disconnect {
  my $self = shift;

  $self->send_request_event({
    type    => 'origin_disconnect',
    request => $self->{'request'},
    connection => $self,
  });
  $self->close(); 
}


sub event_received_rline {
  my $self = shift;
  $self->SUPER::event_received_rline();

  # everything relies on a parsed rline, so do that here so we can
  # fail early if something invalid has been passed in

  # note that rline here is the RESPONSE from an origin server

  # try to parse the rline
  my ($proto, $status, $status_text) = $self->{'request'}->parse_response_rline();
  unless($status && $proto) {
    # fail
    my $server = $self->{'request'}->{'client_connection'}->{'poller'}->{'server'};
    $server->send_request_event({
      type    => 'failed_to_parse_response',
      request => $self->{'request'},
    });
  } else {
    # success
    $self->send_request_event({
      type    => 'received_origin_rline',
      request => $self->{'request'},
    });
  }
}

sub event_received_headers {
  my $self = shift;
  $self->SUPER::event_received_headers();

  return unless ref $self->{'request'};
  $self->send_request_event({
    type    => 'received_origin_headers',
    request => $self->{'request'},
  });
}

sub event_content_data_in {
  my $self = shift;
  $self->SUPER::event_content_data_in();

  return unless ref $self->{'request'};
  $self->send_request_event({
    type    => 'received_origin_data_chunk',
    request => $self->{'request'},
  });
}

sub _close {
  my $self = shift;

  $self->SUPER::_close(@_);

  $self->{'request'} = undef;
}

sub DESTROY {
  my $self = shift;

  $self->{'request'} = undef;
}


1;
__END__

