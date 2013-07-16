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

package MCP::Server::Proxy;
use base 'MCP::Server';
use MCP::Poller;
use MCP::Poller::Connection::Client;
use MCP::Poller::Connection::Origin;
use MCP::RequestModule::StatusPage;
use MCP::RequestModule::Redirector;
use MCP::RequestModule::HeaderStamper;
use MCP::RequestModule::DirectCache;
use MCP::RequestModule::Forwarder;
use MCP::RequestModule::Exceptions;
use MCP::RequestModule::Logger;
use Time::HiRes qw/ gettimeofday /;
use strict;

sub proto { 
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    process_name    => $args->{'process_name'},
    poller          => undef,
    request_modules => {},
    requests        => {},
    listeners       => {},
    event_modules   => {},
  } 
}


sub init {
  my $self = shift;
  $self->SUPER::init();

  # create a poller
  $self->{'poller'} = MCP::Poller->new({ server => $self }); 

  # initialize request modules, store list of modules that support each event
  my $request_module_sequence = $self->{'config'}->{'request_module_sequence'};
  foreach my $request_module (@{$request_module_sequence}) {
    my $request_module_package = "MCP::RequestModule::$request_module";
    my $all_events = $request_module_package->request_module_events();
    $self->{'request_modules'}->{$request_module} = 
      $request_module_package->new({ server => $self });
    $self->{'request_modules'}->{$request_module}->init();
    foreach my $event_name (@{$all_events}) {
      next unless $request_module_package->can($event_name);
      $self->{'event_modules'}->{$event_name} ||= [];
      push(@{$self->{'event_modules'}->{$event_name}}, $request_module);
    }
  } 
}

sub run {
  my($self,$args) = @_;
  $self->s_log(1, 'process '.$self->{'process_name'}." (pid $$) initializing");

  # prepare poller
  $self->{'poller'}->init();

  # start client listeners
  foreach my $listener_config (@{$self->{'config'}->{'listeners'}}) {
    my $listener = MCP::Poller::Connection::Client->new({
      listener_config => $listener_config,
      poller          => $self->{'poller'},
      listener        => $listener_config->{'listener'},
    });
    $self->{'listeners'}->{scalar($listener)} = $listener;
  }

  # set effective user/group
  $self->change_user_group(
    $self->{'config'}->{'server'}->{'user'},
    $self->{'config'}->{'server'}->{'group'});
  $0 = 'mcpd (process '.$self->{'process_name'}.')';

  # start poller
  $self->s_log(1, 'process '.$self->{'process_name'}." (pid $$) polling");
  $self->{'poller'}->run();
}

sub send_request_event {
  my($self,$event) = @_;
  $event->{'time'} = gettimeofday()
    if($event->{'type'} eq 'poll_tick');
#  $self->s_log(3,$event->{'request'}->{'id'}.' send_request_event: '.$event->{'type'})
#    unless($event->{'type'} eq 'poll_tick');

  $event->{'request'}->{'last_request_event'} = $event->{'type'}
    if(ref $event->{'request'});

  # propagate this event to each request_module, in the configured sequence
  my $event_sub = join('_', 'event', $event->{'type'});

  if (! defined ($self->{'event_modules'}->{$event_sub})
	  || ! scalar (@{$self->{'event_modules'}->{$event_sub}})) {
	  print STDERR "unhandled event " . $event->{'type'} .
		  " sent from " . (caller(0))[0] . " \n";
	  return;
  }

  foreach my $request_module (@{$self->{'event_modules'}->{$event_sub}}) {
    my $request_module_object = $self->{'request_modules'}->{$request_module};
    my $r = $request_module_object->$event_sub($event);

    # By design, we should not have nested events.  If at some point
    # in the future we do wish to, we'll need to keep a call stack in
    # the request object to keep track of things.  This is just a note
    # to remind the future coder to make sure to pop the call stack if
    # one of the inner handlers returns -1.

#     print STDERR "event $event_sub"
#		 . " module $request_module"
#		 . " status " . ($event->{'request'}->{'response_status'}
#						 ? $event->{'request'}->{'response_status'}
#						 : '-')
#		 . ($r == -1 ? " r -1\n" : "\n")
#		 if $event_sub ne 'event_poll_tick';

    last if($r == -1); # if an event handler returns -1, stop processing there
  }
}

sub _prepare_signal_handlers {
  my $self = shift;

  my $cleanup_sub = sub {
    my $server = $self;
    $server->s_log(1,'received HUP/INT/TERM/KILL signal, cleaning up');
 
    # close all listeners
    map { $_->close(); } (values %{$server->{'listeners'}});

    # close poller
    $self->{'poller'}->close()
      if ref $self->{'poller'};
    
    # close server log file
    $server->s_log(1,'process '.$server->{'process'}." (pid $$) exiting");
    $server->_close_log_file();

    exit(0);
  };

  $SIG{'HUP'}  = $cleanup_sub;
  $SIG{'INT'}  = $cleanup_sub;
  $SIG{'TERM'} = $cleanup_sub;
  $SIG{'KILL'} = $cleanup_sub;
  $SIG{'PIPE'} = sub {  };
}

1;
__END__

