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

package MCP::Poller::Connection::Memcached;
use base 'MCP::Poller::Connection';
use MCP::Poller;
use Cache::NBMemcached;
use Data::Dumper; #XXX DEBUG
use strict;

use constant MAX_MEMCACHED_KEY_LENGTH => 240;

my $NBMC = undef;
my $NBMC_ARGS = {};
my $CONNECTIONS = {};
my $ZZZ_CONNECTIONS = {};
my $WATCH_CONNECTIONS = {};
my $ACTIVE_REQUESTS = {};

sub _mark_request_active {
  my($caller,$type,$key,$callback_o,$callback_sub) = @_;

  # add callbacks to list of callers waiting on this type/key
  $ACTIVE_REQUESTS->{$type}->{$key} ||= [];
  push(@{$ACTIVE_REQUESTS->{$type}->{$key}}, [$callback_o, $callback_sub]);
}
sub _mark_request_complete {
  my($caller,$type,$key) = @_;

  # grab callbacks for theis type/key and remove key from active list
  my @callbacks = $ACTIVE_REQUESTS->{$type}->{$key}? 
    @{$ACTIVE_REQUESTS->{$type}->{$key}}: ();
  delete $ACTIVE_REQUESTS->{$type}->{$key};
  return @callbacks;
}

sub get_active_request_info {
  my $self = shift;

  return { 
    map { $_ => [ keys(%{$ACTIVE_REQUESTS->{$_}}) ] } 
        (qw/ GET SET DELETE /)
  };
}


sub poll_tick_1s {
  my($self,$now) = @_;

  $NBMC->clear_stuck_socks($now);

  return undef unless $ACTIVE_REQUESTS->{'GET'};
  my $cache_config =
    $self->{'poller'}->{'server'}->{'config'}->{'cache_server'};
  my $max_pending_gets = $cache_config->{'max_pending_gets'};
  if(scalar keys(%{$ACTIVE_REQUESTS->{'GET'}}) > $max_pending_gets) {
    #my $diag = "\nAR:\n".Data::Dumper->Dump([$ACTIVE_REQUESTS]).
    #           "\nCS:\n".Data::Dumper->Dump([$NBMC->get_cache_socket_info()]).
    #           "\n--\n";
    $self->{'poller'}->{'server'}->s_log(1,
      'Connection::Memcached: too many pending cache gets, exiting.');
    exit();
  }

  return undef;
}

sub set_cache_args {
  my($caller,$args) = @_;
  $NBMC_ARGS = $args;
}

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    socketname            => undef,
  }
}

sub init {
  my($self, $args) = @_;

  $self->{'socket'} = $args->{'socket'};
  $self->{'poller'} = MCP::Poller->get_poller();
  $self->{'socketname'} = scalar($self->{'socket'});
  $CONNECTIONS->{$self->{'socketname'}} = $self;
  #print STDERR __PACKAGE__."->init()  CONNECTIONS=".scalar(keys(%{$CONNECTIONS}))."\n";
}

sub _prepare_cache {
  my $caller = shift;
  return if ref $NBMC;

  # clear existing connection list
  $CONNECTIONS = {};
  $ZZZ_CONNECTIONS = {};

  # start memcached instance
  $NBMC_ARGS->{'nonblocking'} = 1;
  $NBMC_ARGS->{'connection_module'} = $caller;
  $NBMC_ARGS->{'stuck_sock_timeout'} = 1;
  $NBMC = Cache::NBMemcached->new($NBMC_ARGS);
  #print STDERR "Cache::NBMemcached->new (NBMC=$NBMC)\n";

  # update socket watch list
#  $caller->_update_watch_list();
}

sub _update_watch_list {
  my $caller = shift;

  $caller->_prepare_cache();
  my $socks = $NBMC->get_watch_socks();

  # create new objects for novel sockets
  my $socks_found = {};
  foreach my $sock (values %{$socks->{'watch_read'}}, values %{$socks->{'watch_write'}}) {
    $socks_found->{$sock} = 1; 
    unless(exists $CONNECTIONS->{$sock}) {
      if(exists $ZZZ_CONNECTIONS->{$sock}) {
        $CONNECTIONS->{$sock} = $ZZZ_CONNECTIONS->{$sock};
        delete $ZZZ_CONNECTIONS->{$sock};
      }
      else {
        $caller->new({ socket => $sock });
      }
    }
  }
  
  foreach my $connection (values %{$CONNECTIONS}) {
    # forget objects for missing sockets
    unless(exists $socks_found->{$connection->{'socketname'}}) {
       $ZZZ_CONNECTIONS->{$connection->{'socketname'}} = $connection;
       delete $CONNECTIONS->{$connection->{'socketname'}};
#      $CONNECTIONS->{$connection->{'socketname'}}->close();
    }

    # check read/write state of extant sockets
    if($connection->{'watching_read'}) {
      $connection->unwatch_read()
        unless(exists $socks->{'watch_read'}->{$connection->{'socket'}});
    }
    else {
      $connection->watch_read()
        if(exists $socks->{'watch_read'}->{$connection->{'socket'}});
    }
    if($connection->{'watching_write'}) {
      $connection->unwatch_write()
        unless(exists $socks->{'watch_write'}->{$connection->{'socket'}});
    }
    else {
      $connection->watch_write()
        if(exists $socks->{'watch_write'}->{$connection->{'socket'}});
    }
  }
}

sub enqueue_set {
  my($caller,$callback_o,$callback_sub,$key,$value,$ttl) = @_;
  $key = substr($key,0,MAX_MEMCACHED_KEY_LENGTH)
    if(length($key) > MAX_MEMCACHED_KEY_LENGTH);

  $caller->_prepare_cache();
  $NBMC->set($key,$value,$ttl); 
#  $caller->_mark_request_active('SET',$key,$callback_o,$callback_sub);
##  $caller->_update_watch_list();
}

sub enqueue_get {
  my($caller,$callback_o,$callback_sub,@keys_in) = @_;
  my @keys; 
  foreach my $key (@keys_in) {
    $key = substr($key,0,MAX_MEMCACHED_KEY_LENGTH)
      if(length($key) > MAX_MEMCACHED_KEY_LENGTH);
    push(@keys, $key);
  }

  $caller->_prepare_cache();
  $NBMC->get_multi(@keys); 
  map { $caller->_mark_request_active('GET',$_,$callback_o,$callback_sub) } 
      @keys;
##  $caller->_update_watch_list();
}

sub enqueue_delete {
  my($caller,$callback_o,$callback_sub,$key,$time) = @_;
  $key = substr($key,0,MAX_MEMCACHED_KEY_LENGTH)
    if(length($key) > MAX_MEMCACHED_KEY_LENGTH);

  $caller->_prepare_cache();
  $NBMC->delete($key,$time); 
  $caller->_mark_request_active('DELETE',$key,$callback_o,$callback_sub);
##  $caller->_update_watch_list();
}


sub event_ready_for_read  { shift->_ready_for_rw(@_); }
sub event_ready_for_write { shift->_ready_for_rw(@_); }
sub _ready_for_rw {
  my $self = shift;
  $self->_prepare_cache();
  my $responses = $NBMC->sock_ready($self->{'socket'});
  $self->_handle_responses($responses);
##  $self->_update_watch_list();
}

sub _handle_responses {
  my($self,$responses) = @_;

  foreach my $response (@{$responses}) {
    if($response->{'type'} =~ /^STORED/o) {
#      my @callbacks = 
#        $self->_mark_request_complete('SET', $response->{'key'});
#      foreach my $callback (@callbacks) { 
#        my($callback_o,$callback_sub) = @{$callback}; 
#        if(ref $callback_sub) {
#          $callback_sub->('STORED',$response->{'key'});
#        }
#        else {
#          $callback_o->$callback_sub('STORED',$response->{'key'});
#        }
#      }
    }
    elsif($response->{'type'} =~ /^NOT_STORED/o) {
#      my @callbacks = 
#        $self->_mark_request_complete('SET', $response->{'key'});
#      foreach my $callback (@callbacks) { 
#        my($callback_o,$callback_sub) = @{$callback}; 
#        if(ref $callback_sub) {
#          $callback_sub->('NOT_STORED',$response->{'key'});
#        }
#        else {
#          $callback_o->$callback_sub('NOT_STORED',$response->{'key'});
#        }
#      }
    }
    elsif($response->{'type'} eq 'HIT') {
      my @callbacks = 
        $self->_mark_request_complete('GET', $response->{'key'});
      foreach my $callback (@callbacks) { 
        my($callback_o,$callback_sub) = @{$callback}; 
        # for scalars, make a private copy when there are multiple callbacks
        my $value = $response->{'value'};
        if((ref($value) eq 'SCALAR') && $#callbacks) {
          my $s = ${$value};
          $value = \$s;
        }

        if(ref $callback_sub) {
          $callback_sub->('HIT',$response->{'key'},$value);
        }
        else {
          $callback_o->$callback_sub('HIT',
            $response->{'key'},$value);
        }
      }
    }
    elsif($response->{'type'} eq 'MISS') {
      my @callbacks = 
        $self->_mark_request_complete('GET', $response->{'key'});
      foreach my $callback (@callbacks) { 
        my($callback_o,$callback_sub) = @{$callback}; 
        if(ref $callback_sub) {
          $callback_sub->('MISS',$response->{'key'});
        }
        else {
          $callback_o->$callback_sub('MISS',$response->{'key'});
        }
      }
    }
    elsif($response->{'type'} =~ /^DELETED/o) {
      my @callbacks = 
        $self->_mark_request_complete('DELETE', $response->{'key'});
      foreach my $callback (@callbacks) { 
        my($callback_o,$callback_sub) = @{$callback}; 
        if(ref $callback_sub) {
          $callback_sub->('DELETED',$response->{'key'});
        }
        else {
          $callback_o->$callback_sub('DELETED',$response->{'key'});
        }
      }
    }
    elsif($response->{'type'} =~ /^NOT_FOUND/o) {
      my @callbacks = 
        $self->_mark_request_complete('DELETE', $response->{'key'});
      foreach my $callback (@callbacks) { 
        my($callback_o,$callback_sub) = @{$callback}; 
        if(ref $callback_sub) {
          $callback_sub->('NOT_FOUND',$response->{'key'});
        }
        else {
          $callback_o->$callback_sub('NOT_FOUND',$response->{'key'});
        }
      }
    }
    elsif($response->{'type'} =~ /^SERVER_ERROR/o) {
      $self->{'poller'}->{'server'}->s_log(2,"memcached: SERVER_ERROR k=".$response->{'key'}." (".$response->{'cmd'}.")");
      $self->_mark_request_complete($response->{'cmd'}, $response->{'key'})
        if($response->{'cmd'});
    }
    #TODO increment, etc.
    else {
      print STDERR "unknown response type: ".$response->{'type'}."\n";
      #TODO ???
    }
  }
}

sub event_hangup { #TODO ??? 
}

#TODO dead sockets must be unwatched by the poller!

sub event_disconnect {
  my $self = shift;

  #TODO ???
}

sub close {
  my $self = shift;

  $self->unwatch_all();

  delete $CONNECTIONS->{$self->{'socketname'}};
}

sub DESTROY {
  my $self = shift;

  $self->unwatch_all();

  delete $CONNECTIONS->{$self->{'socketname'}};
  #TODO ???
}

sub get_cache_socket_info {
  my $self = shift;

  return $NBMC->get_cache_socket_info();
}


#
# callbacks for Cache::NBMemcached to use
#
sub sock_new { 
  my($p,$sock) = @_;
  if(exists $ZZZ_CONNECTIONS->{$sock}) {
    $CONNECTIONS->{$sock} = $ZZZ_CONNECTIONS->{$sock};
    delete $ZZZ_CONNECTIONS->{$sock};
  }
  else {
    $p->new({ 'socket' => $sock });
  }
}
    
sub sock_delete {
  my($p,$sock) = @_;
  if(my $c = $CONNECTIONS->{$sock}) {
    $c->unwatch_all();
    $ZZZ_CONNECTIONS->{$c->{'socketname'}} = $c;
    delete $CONNECTIONS->{$c->{'socketname'}};
  }
}
    
sub sock_watch {
  my($p,$sock,$r,$w) = @_;

  if(my $c = $CONNECTIONS->{$sock}) {
    if($r) {
      $c->watch_read();
    }
    else {
      $c->unwatch_read();
    }
    if($w) {
      $c->watch_write();
    }
    else {
      $c->unwatch_write();
    }
  } 
}


1;
__END__

