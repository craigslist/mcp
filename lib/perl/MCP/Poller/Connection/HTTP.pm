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

package MCP::Poller::Connection::HTTP;
use base 'MCP::Poller::Connection';
use strict;

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    'raw_rline'        => '',
    'raw_headers'      => '',
    'raw_content'      => '',
    'received_rline'   => 0,
    'received_headers' => 0,
    'received_content' => 0,
  } 
}


sub event_ready_for_read {
  my $self = shift;

  # let raw reading and buffer population happen first
  $self->SUPER::event_ready_for_read(@_);

  # we care not if the read_buffer is empty
  return unless length $self->{'read_buffer'};

  # if we've yet to receive the request line, anticipate that in buffer
  unless($self->{'received_rline'}) {
    $self->{'raw_rline'} .= $self->{'read_buffer'};
    $self->{'read_buffer'} = '';

    # buffer contains full rline (and maybe more)
    if($self->{'raw_rline'} =~ /\r\n/so) {
      ($self->{'raw_rline'}, $self->{'read_buffer'}) = 
        split(/\r\n/so, $self->{'raw_rline'}, 2);
      $self->event_received_rline();
    } 
    elsif($self->{'raw_rline'} =~ /\n/so) {
      ($self->{'raw_rline'}, $self->{'read_buffer'}) = 
        split(/\n/so, $self->{'raw_rline'}, 2);
      $self->event_received_rline();
    } 
    
  }

  # if it is time for headers, anticipate that in buffer
  if($self->{'received_rline'} && !$self->{'received_headers'}) {
    $self->{'raw_headers'} .= $self->{'read_buffer'};
    $self->{'read_buffer'} = '';

    # buffer contains full headers (and maybe more)
    if($self->{'raw_headers'} =~ /\r\n\r\n/so) {
      ($self->{'raw_headers'}, $self->{'read_buffer'}) =
        split(/\r\n\r\n/so, $self->{'raw_headers'}, 2);
      $self->event_received_headers();
    } 
    elsif($self->{'raw_headers'} =~ /\n\n/so) {
      ($self->{'raw_headers'}, $self->{'read_buffer'}) =
        split(/\n\n/so, $self->{'raw_headers'}, 2);
      $self->event_received_headers();
    } 
    
  }

  # if we already have rline and headers, the rest must be content
  if($self->{'received_rline'} && $self->{'received_headers'}) {
    $self->{'raw_content'} .= $self->{'read_buffer'};
    $self->{'read_buffer'} = '';
    $self->event_content_data_in();
  }

}


sub event_received_rline {
  shift->{'received_rline'} = 1; 
}

sub event_received_headers {
  shift->{'received_headers'} = 1; 
}

sub event_content_data_in { }

1;
__END__

