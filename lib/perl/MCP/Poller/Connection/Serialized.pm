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

package MCP::Poller::Connection::Serialized;
use base 'MCP::Poller::Connection';
use strict;

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    'proto' => undef,
    'proto_chunk_buf' => '',
  }
}


sub event_ready_for_read {
  use bytes;
  my $self = shift;
  my $p = $self->{'proto'};

  # let raw reading and buffer population happen first
  $self->SUPER::event_ready_for_read(@_);

  # we care not if the read_buffer is empty
  return unless length $self->{'read_buffer'};

  $self->{'proto_chunk_buf'} .= $self->{'read_buffer'};
  $self->{'read_buffer'} = '';
  while(length($self->{'proto_chunk_buf'})) {
    my $bytes_wanted = $p->bytes_wanted();
#print STDERR "bytes wanted: $bytes_wanted (".length($self->{'proto_chunk_buf'}).")\n";

    # message is ready, send connection event
    if($bytes_wanted == 0) {
      $self->event_proto_message_ready();
      next;
    }
   
    # we have a enough data for the protocol object to process
    if(length($self->{'proto_chunk_buf'}) >= $bytes_wanted) {
      my $buf = substr($self->{'proto_chunk_buf'},0,$bytes_wanted,'');
#print STDERR "processing ".length($buf)." bytes\n";
      $p->process_bytes($buf);
#print STDERR "done.\n";
 
      # if no further data needed, the message is ready
      unless($p->bytes_wanted()) {
#print STDERR "no more bytes wanted, send proto_message_ready\n";
        $self->event_proto_message_ready();
        next;
      }
    }

    # not enough data yet, exit event
    else {
      last;
    }
  }
}




1;
__END__

