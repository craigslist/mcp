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

package MCP::Poller::Connection;
use base 'MCP::Object';
use Fcntl qw(SEEK_CUR SEEK_SET SEEK_END);
#use Devel::Leak::Object qw(GLOBAL_bless);
use strict;


use constant DEFAULT_READ_BUFFER_CHUNK_SIZE  => 16 * 1024;
use constant DEFAULT_WRITE_BUFFER_CHUNK_SIZE => 0;


sub proto { 
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    'poller'                  => $args->{'poller'},
    'socket'                  => $args->{'socket'} || undef,
    'watching_read'           => 0,
    'watching_write'          => 0,
    'write_buffer'            => '',
    'read_buffer'             => '',
    'close_after_write'       => 0,
    'bytes_out'               => 0,
    'read_buffer_chunk_size'  => DEFAULT_READ_BUFFER_CHUNK_SIZE,
    'write_buffer_chunk_size' => DEFAULT_WRITE_BUFFER_CHUNK_SIZE,
    'is_closed'               => 0,
    'bytes_read'              => 0,
    'bytes_written'           => 0,
    'throttle_delay'          => 0, # number of ticks to wait between writes
    'throttle_counter'        => 0, # for the above (counts down with action on zero)
  }
}

sub init { 
  my $self = shift;

  my $config = $self->{'poller'}->{'server'}->{'config'}->{'poller'};
  $self->{'read_buffer_chunk_size'} = $config->{'read_buffer_chunk_size'}
    if $config->{'read_buffer_chunk_size'};
  $self->{'write_buffer_chunk_size'} = $config->{'write_buffer_chunk_size'}
    if $config->{'write_buffer_chunk_size'};
}


sub event_ready_for_read {
  use bytes;
  my $self = shift;
  my $chunk_size = $self->{'read_buffer_chunk_size'};

  # read all incoming data on this socket and put it in the read buffer
  my $bytes_read = 0;
  if(sysread($self->{'socket'}, my $buf, $chunk_size) > 0) {
    $self->{'read_buffer'} .= $buf;
    $bytes_read += length($buf);
  }
  $self->{'bytes_read'} += $bytes_read;

  $self->event_disconnect() 
    unless $bytes_read ;
  return;
}

sub event_ready_for_write {
  use bytes;
  my $self = shift;
  my $chunk_size = $self->{'write_buffer_chunk_size'};

  # if socket looks to have disconnected, give up.
  if( (ref($self->{'socket'}) eq 'IO::Socket::INET') &&
      !$self->{'socket'}->peerhost() ) {
    $self->{'poller'}->{'server'}->s_log(3,"unexpected disconnect");
    $self->_close();
    return;
  }

  # if pending outbound data exists, write it
  if(my $len = length($self->{'write_buffer'})) {
    $len = $chunk_size if $chunk_size;
    my $written = syswrite($self->{'socket'}, $self->{'write_buffer'}, $len); 
    $self->{'bytes_written'} += $written;
    if($written == length($self->{'write_buffer'})) {
      $self->{'write_buffer'} = '';
    }
    else {
      substr($self->{'write_buffer'}, 0, $written, '');
    }
  }

  if (length $self->{'write_buffer'}) {
	  if ($self->{'throttle_delay'}) {
		  # send the event that says we've jsut written a throttled
		  # data chunk; this will be caught by the Shaper module and
		  # this connection will be scheduled for a timeout
		  $self->send_request_event({ type => 'sent_client_data_chunk_throttled' });
	  }
	  # in both the rate-limited and non-, the buffer is still not
	  # emptied, so simply return and we'll be called again when this
	  # fd pops up in the epolld loop
	  return;
  }

  # write buffer empty, stop polling for write-readiness
  $self->unwatch_write();

  # if we're waiting to close, do that now
  if($self->{'close_after_write'}) {
    $self->_close();
  }
}

sub event_hangup { 
  my $self = shift;
  return $self->_close(); 
}
sub event_error { 
  my $self = shift;
  return $self->_close(); 
}

sub event_disconnect { }

sub write_data {
  my($self, undef) = @_;  # pass data as scalarref or plain old scalar

  # put data in the write buffer
#  $self->{'write_buffer'} .= $_[1]; 
  $self->{'write_buffer'} .= (ref($_[1]) eq 'SCALAR')? ${$_[1]}: $_[1]; 

  # remember to write it to the socket when it becomes ready
  $self->watch_write();
}

sub close {
  my $self = shift;
  return undef if $self->{'is_closed'};

  # if there's pending data to write, let it out before closing
  if(length $self->{'write_buffer'}) {
    $self->{'close_after_write'} = 1;
    $self->unwatch_read();
  }

  # no pending data to write, really close
  else {
    $self->_close();
  }
}

sub _close {
  my $self = shift;
  return undef if $self->{'is_closed'};
  $self->{'close_after_write'} = 0;

  # forget about this connection
  $self->unwatch_all();

  # close it
  $self->{'socket'}->close();
  $self->{'poller'}->{'server'}->s_log(4,"socket closed");
  $self->{'is_closed'} = 1;
}


sub watch_read { 
  my $self = shift;
  
  my $watch_sub = $self->{'watching_write'}? 'watch_read_write': 'watch_read';
  $self->{'poller'}->$watch_sub( $self );
  $self->{'watching_read'} = 1;
}

sub watch_write {
  my $self = shift;

  my $watch_sub = $self->{'watching_read'}? 'watch_read_write': 'watch_write';
  $self->{'poller'}->$watch_sub( $self );
  $self->{'watching_write'} = 1;
}

sub unwatch_read { 
  my $self = shift;
  
  my $watch_sub = $self->{'watching_write'}? 'watch_write': 'unwatch';
  $self->{'poller'}->$watch_sub( $self );
  $self->{'watching_read'} = 0;
}

sub unwatch_write {
  my $self = shift;

  my $watch_sub = $self->{'watching_read'}? 'watch_read': 'unwatch';
  $self->{'poller'}->$watch_sub( $self );
  $self->{'watching_write'} = 0;
}

sub unwatch_all {
  my $self = shift;
  $self->{'poller'}->unwatch( $self );
  $self->{'watching_read'} = 0;
  $self->{'watching_write'} = 0;
}

sub send_request_event {
  my($self,$event) = @_;

  $self->{'poller'}->{'server'}->send_request_event({ 
    connection => $self,
    %{ $event },
  });
}

sub poll_tick_1s { }

1;
__END__

