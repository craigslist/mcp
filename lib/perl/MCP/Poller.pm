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

package MCP::Poller;
use base 'MCP::Object';
use MCP::Request;
use IO::Epoll;
use Fcntl qw(F_SETFL O_NONBLOCK);
use POSIX;
use Errno;
use Data::Dumper;
use strict;


# singleton behavior
{
  my $_poller;
  sub get_poller { $_poller }
  sub _set_poller { shift; $_poller = shift }
}


sub proto { 
  my($caller,$args) = @_;
  my $config = $args->{'server'}->{'config'}->{'poller'};
  return {
    %{ shift->SUPER::proto(@_) },
    server      => $args->{'server'},
    connections => {},
    epoll       => epoll_create($config->{'epoll_create_number'}),
    last_sched  => scalar(time()),
  } 
}

sub init { 
  my $self = shift;
  $self->_set_poller($self);
}

sub run {
  my $self = shift;
  my $config = $self->{'server'}->{'config'}->{'poller'};

  # this is the main poll loop
  while (1) {
	if(my $epoll_events = 
	  epoll_wait($self->{'epoll'}, 
				 $config->{'epoll_event_chunk_size'},
				 $config->{'tick_milliseconds'})) {
	  foreach my $epoll_event (@{$epoll_events}) {
		my($file_number,$modes) = @{$epoll_event};
		my $connection = $self->{'connections'}->{$file_number};

		# these are the basic connection events
		if(ref $connection) {
		  ($modes & EPOLLERR) && do { $connection->event_error();  next; };
		  ($modes & EPOLLHUP) && do { $connection->event_hangup(); next; };
		  ($modes & EPOLLOUT) && do { $connection->event_ready_for_write(); };
		  ($modes & EPOLLIN)  && do { $connection->event_ready_for_read(); };
		}

		# dead connection, unwatch please
		else {
		  epoll_ctl($self->{'epoll'},EPOLL_CTL_DEL,$file_number,undef);
		}
	  }
	  $self->poll_tick(scalar @{$epoll_events});
    } else {
	    if ($!) {
		    # die on all errors that aren't retries
		    unless ($! == Errno::EINTR
		            || $! == Errno::EINVAL) {
			    $self->s_log(1, "epoll_wait: ".$!);
			    die "epoll_wait: ".$!;
		    }
		    # spew log the error if at the appropriate debug level
		    $self->{'server'}->s_log(3,"epoll_wait: ".$!);
	    }
    }
  }
}

sub watch_read  { shift->_connection_watch(EPOLLIN|EPOLLHUP|EPOLLERR,@_); }
sub watch_write { shift->_connection_watch(EPOLLOUT|EPOLLHUP|EPOLLERR,@_); }
sub watch_read_write {
  shift->_connection_watch(EPOLLOUT|EPOLLIN|EPOLLHUP|EPOLLERR,@_); }
sub unwatch     { shift->_connection_watch(undef,@_); }

sub _connection_watch {
  my($self,$mode,$connection) = @_;
  my $socket      = $connection->{'socket'};
  return undef unless $socket;
  my $file_number = fileno $socket;

  # delete connection
  unless(defined $mode) {
    delete $self->{'connections'}->{$file_number};
    return epoll_ctl($self->{'epoll'},EPOLL_CTL_DEL,$file_number,undef);
  }

  # modify extant connection
  if(exists $self->{'connections'}->{$file_number}) {
    return epoll_ctl($self->{'epoll'},EPOLL_CTL_MOD,$file_number,$mode);
  }

  # add new connection
  else {
    $self->{'connections'}->{$file_number} = $connection;
    my $flags = fcntl($socket, F_SETFL, O_NONBLOCK);
    return epoll_ctl($self->{'epoll'},EPOLL_CTL_ADD,$file_number,$mode);
  }  
}

sub poll_tick {
  my($self, $events) = @_;

  # 1s tick gets sent to connection objects in case they want timeouts, etc
  if((my $now = scalar(time())) > $self->{'last_sched'}) {
    my $server = $self->{'server'};
    map { $_->poll_tick_1s($now) } values(%{$self->{'connections'}});
    map { $_->poll_tick_1s($now) } values(%{$server->{'request_modules'}});
    $self->{'last_sched'} = $now;
    $self->{'server'}->s_log(3,"connections: ".scalar(keys(%{$self->{'connections'}})));
  }

  # issue tick event
  $self->{'server'}->send_request_event({
    type       => 'poll_tick',
    num_events => $events,
  });
}

sub close {
  my $self = shift;
  
  # shut down poller
  POSIX::close($self->{'epoll'});

}

1;
__END__

