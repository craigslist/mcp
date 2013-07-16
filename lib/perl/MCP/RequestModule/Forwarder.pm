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

package MCP::RequestModule::Forwarder;
use base 'MCP::RequestModule';
use MCP::WeightedDice;
use Time::HiRes qw/ gettimeofday /;
use strict;


sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    pool_state => {},
    host_state => {},
    pool_dice  => {},
    initial_weights => {},
    pending_connections      => {},
    pending_connection_times => {},
  }
}

sub init {
  my $self = shift;
  my $config = $self->{'server'}->{'config'}->{'forwarder'};

  # build pool state
  foreach my $pool (keys %{$config->{'origin_pools'}}) {
    $self->{'pool_state'}->{$pool} = 1;
  }

  $self->_initialize_pool_state($config);
}

sub _initialize_pool_state {
  my($self,$config) = @_;
  my $max_confidence =
      $self->{'server'}->{'config'}->{'forwarder'}->{'host_max_confidence'};

  # build initial host state 
  foreach my $pool (keys %{$config->{'origin_pools'}}) {
    my $host_weights = {};
    foreach my $pool_member (@{$config->{'origin_pools'}->{$pool}}) {
        if(ref($pool_member) eq 'ARRAY') {
            $host_weights->{$pool_member->[0]} = $pool_member->[1];
        }
        else {
            $host_weights->{$pool_member} = $max_confidence;
        }
    }
    $self->{'host_state'}->{$pool} = $host_weights;
    $self->{'initial_weights'}->{$pool} = { %{$host_weights} };
  }

  # initialize WeightedDice 
  foreach my $pool (keys %{$self->{'host_state'}}) {
    $self->{'pool_dice'}->{$pool} = 
      MCP::WeightedDice->new($self->{'host_state'}->{$pool});
  }
}

sub event_received_client_headers {
  my($self,$event) = @_;

  # don't attempt origin connect if we're still buffering
  return undef if($event->{'request'}->{'buffer_content_data'});

  #XXX TIMER BEGIN forward_request
  $event->{'request'}->{'timers'}->{'forward_request'} = gettimeofday();

  # open connection to origin server
  return -1 if($self->_attempt_origin_connect($event->{'request'}) == -1);
  #TODO end request if _attempt_origin_connect returns undef?
}

sub event_received_cache_miss {
  my($self,$event) = @_;

  # don't attempt origin connect if we're still buffering
  return undef if($event->{'request'}->{'buffer_content_data'});

  #XXX TIMER BEGIN forward_request
  $event->{'request'}->{'timers'}->{'forward_request'} = gettimeofday();

  # open connection to origin server
  return -1 if($self->_attempt_origin_connect($event->{'request'}) == -1);
  #TODO end request if _attempt_origin_connect returns undef?
}

sub event_origin_connect {
  my($self,$event) = @_;
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};
  my $client_connection = $request->{'client_connection'};

  # origin connection is ready, clear from pending list 
  $request->{'origin_connect_pending'} = 0;
  delete $self->{'pending_connections'}->{scalar $origin_connection};
  delete $self->{'pending_connection_times'}->{scalar $origin_connection};

  # increase confidence, if applicable
  if(!$request->{'origin_retry_bad_status'}) {
    $self->increase_host_confidence(
      $origin_connection->{'pool'}, $origin_connection->{'connect_to'});
  }

  # send request and headers, watch for read
  $origin_connection->{'sending_request'} = 1;
  $origin_connection->watch_read();
  $origin_connection->write_data(
    $request->get_rline()."\r\n".
    $request->get_header_string()."\r\n\r\n"
  );
  $self->{'server'}->s_log(2, "forward request:\n". 
    $request->get_rline()."\r\n".
    $request->get_header_string()."\r\n\r\n");

  # if there's request content, send that along, also
  if(length $client_connection->{'raw_content'}) {
    $origin_connection->write_data($client_connection->{'raw_content'});
    $client_connection->{'raw_content'} = '';
  }
  $request->{'buffer_content_data'} = 0;

}

sub event_poll_tick {
  my $self = shift;
  my $config = $self->{'server'}->{'config'}->{'forwarder'};
  my $now = scalar(time());

  # look for connection attempts timing out and clear them
  my $origin_connect_timeout = $config->{'origin_connect_timeout'};
  foreach my $conn (keys %{$self->{'pending_connections'}}) {
    my $start_time = $self->{'pending_connection_times'}->{$conn};
    next unless(($now - $start_time) >= $origin_connect_timeout);
    $self->{'pending_connections'}->{$conn}->event_connect_fail();
  }
}

sub event_origin_connect_fail {
  my($self,$event) = @_;
  my $config            = $self->{'server'}->{'config'}->{'forwarder'};
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};

  # clear this from pending connection list
  $request->{'origin_connect_pending'} = 0;
  delete $self->{'pending_connections'}->{scalar $origin_connection};
  delete $self->{'pending_connection_times'}->{scalar $origin_connection};

  # decrease host confidence, log failure
  $self->decrease_host_confidence(
    $origin_connection->{'pool'}, $origin_connection->{'connect_to'});
  $self->{'server'}->s_log(2,
    'failed to connect to host '.$origin_connection->{'connect_to'});
 
  # retry connection if max attempts not exceeded
  my $connection_attempts = $request->{'origin_connect_attempts'};
  if($connection_attempts < $config->{'origin_connect_attempts'}) {
    $origin_connection->_close();
    $request->{'origin_connection'} = undef;
    return -1 if($self->_attempt_origin_connect($request) == -1);
  }

  # too many attempts, give up
  else {
    $origin_connection->_close();

    $self->{'server'}->send_request_event({
      type    => 'origin_connect_max_retries',
      request => $request,
    });
  }

}

sub event_origin_connect_max_retries { 
  my($self,$event) = @_;
  my $config = $self->{'server'}->{'config'}->{'forwarder'};
  my $request = $event->{'request'};
  my $response_type = $request->{'origin_max_retries_event'} ||
                      'origin_connect_max_retries';
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{$response_type});
  return -1;
}

sub event_received_client_data_chunk {
  my($self,$event) = @_;
  my $request           = $event->{'request'};
  my $client_connection = $event->{'connection'};
  my $origin_connection = $request->{'origin_connection'};

  # in buffer mode, if we've buffered sufficient data, attempt origin connect
  if($request->{'buffer_content_data'} &&
     ( length($client_connection->{'raw_content'}) >=
       $request->{'buffer_content_data'} ) ) {
    return -1 if($self->_attempt_origin_connect($request) == -1);
  }

  # if we're in the middle of submitting a request, pass incoming data along
  if($origin_connection->{'sending_request'}) {
    $origin_connection->write_data($client_connection->{'raw_content'});
    $client_connection->{'raw_content'} = '';
  }
}

sub event_received_origin_rline {
  my($self,$event) = @_;
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};
  my $client_connection = $request->{'client_connection'};

  if($request->{'origin_retry_bad_status'}) {
    if ($request->{'response_status'} =~ /^[45]/) {
      $self->decrease_host_confidence(
        $origin_connection->{'pool'}, $origin_connection->{'connect_to'});
      $self->{'server'}->s_log(2, 
        "origin server bad status for request $request");
      # retry connection if max attempts not exceeded
      my $config = $self->{'server'}->{'config'}->{'forwarder'};
      my $connection_attempts = $request->{'origin_connect_attempts'};
      if($connection_attempts < $config->{'origin_connect_attempts'}) {
        $origin_connection->_close();
        $request->{'origin_connection'} = undef;
        $self->_attempt_origin_connect($request);
        return -1;
      }
    }
    else {
      $self->increase_host_confidence(
        $origin_connection->{'pool'}, $origin_connection->{'connect_to'});
    }
  }

  $request->{'sent_response_data'} = 1;
  $client_connection->write_data($request->get_response_rline()."\r\n");

  # if we get here, we are serving the request to the client,
  # so claim responsibility for doing so
  $request->register_response({
      status         => $request->{'response_status'},
      status_text    => $request->{'response_status_text'},
  });
}

sub event_received_origin_headers {
  my($self,$event) = @_;
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};
  my $client_connection = $request->{'client_connection'};

  $client_connection->write_data(
    $request->get_response_header_string()."\r\n\r\n"
  );
}

sub event_received_origin_data_chunk {
  my($self,$event) = @_;
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};
  my $client_connection = $request->{'client_connection'};

  $client_connection->write_data($origin_connection->{'raw_content'});
  $origin_connection->{'content_length'} += 
      length($origin_connection->{'raw_content'});
  $origin_connection->{'raw_content'} = '';
}

sub event_origin_disconnect {
  my($self,$event) = @_;
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};

  #XXX TIMER END fixup
  $request->{'timers'}->{'fixup'} = 
    gettimeofday() - $request->{'timers'}->{'fixup'};
  #XXX TIMER END forward_request
  $request->{'timers'}->{'forward_request'} = 
    gettimeofday() - $request->{'timers'}->{'forward_request'};

  # remember response content length
  $request->{'response_content_length'} = 
      $origin_connection->{'content_length'};

  # see if data was sent.  if so, close connection, if not, exception event
  if($request->{'sent_response_data'}) {
    $self->{'server'}->s_log(2, "closing client connection");
    my $client_connection = $request->{'client_connection'};
    $client_connection->close()
      if ref $client_connection;
    $self->{'server'}->s_log(2, "done trying to close client connection");
  }
  else {
    if($request->{'method'} eq 'GET') {
      $self->{'server'}->s_log(2, 
        "origin server disconnect after request $request sent, retrying...");
      $self->{'server'}->send_request_event({
        type    => 'origin_connect_fail',
        request => $request,
        connection => $request->{'origin_connection'},
      });
      return -1;
    }
    else {
      $self->{'server'}->s_log(2, "failed to parse origin response");
      # FIXME: this is likely the wrong place to send this event
      $self->{'server'}->send_request_event({
        type    => 'failed_to_parse_response',
        request => $request,
      });
    }
  }
}


sub decrease_host_confidence {
  my($self,$pool,$host) = @_;
  my $config = $self->{'server'}->{'config'}->{'forwarder'};
  my $host_min_confidence = $config->{'host_min_confidence'};
  my $decrease_factor     = $config->{'host_confidence_decrease_factor'};

  # skip if confidence already at minimum
  my $cur_confidence = $self->{'host_state'}->{$pool}->{$host};
  return $cur_confidence if($cur_confidence <= $host_min_confidence);

  # decrease confidence by factor.  update host_state and the dice object
  my $new_confidence = $cur_confidence * $decrease_factor;
  $self->{'host_state'}->{$pool}->{$host} = $new_confidence;
  $self->{'pool_dice'}->{$pool}->adjust_key($host, $new_confidence);

  $self->{'server'}->s_log(2, 
    "confidence: $pool,$host: $cur_confidence -> $new_confidence");

  # commented out for now, since no other module handles
  # this event, but left here for future use

  # # confidence changed, issue event
  # $self->{'server'}->send_request_event({
  #   type => 'origin_host_confidence_change',
  #   pool => $pool,
  #   host => $host,
  #   old_confidence => $cur_confidence,
  #   new_confidence => $new_confidence,
  # });

  # commented out for now, since no other module handles
  # this event, but left here for future use

  # # issue event if confidence has hit bottom
  # if($new_confidence <= $host_min_confidence) {
  #   $self->{'server'}->send_request_event({
  #     type => 'origin_host_min_confidence',
  #     pool => $pool,
  #     host => $host,
  #   });
  # }

  return $new_confidence;
}

sub increase_host_confidence {
  my($self,$pool,$host) = @_;
  my $config = $self->{'server'}->{'config'}->{'forwarder'};
  my $host_max_confidence = $self->{'initial_weights'}->{$pool}->{$host};
  my $increase_factor     = $config->{'host_confidence_increase_factor'};

  # skip if confidence already at maximum
  my $cur_confidence = $self->{'host_state'}->{$pool}->{$host};
  return $cur_confidence if($cur_confidence >= $host_max_confidence);

  # increase confidence by factor.  update host_state and the dice object
  my $new_confidence = $cur_confidence * $increase_factor;
  $self->{'host_state'}->{$pool}->{$host} = $new_confidence;
  $self->{'pool_dice'}->{$pool}->adjust_key($host, $new_confidence);

  $self->{'server'}->s_log(2, 
    "confidence: $pool,$host: $cur_confidence -> $new_confidence");

  # commented out for now, since no other module handles
  # this event, but left here for future use

  # # confidence changed, issue event
  # $self->{'server'}->send_request_event({
  #   type => 'origin_host_confidence_change',
  #   pool => $pool,
  #   host => $host,
  #   old_confidence => $cur_confidence,
  #   new_confidence => $new_confidence,
  # });

  # commented out for now, since no other module handles
  # this event, but left here for future use

  # # issue event if confidence has hit top 
  # if($new_confidence >= $host_max_confidence) {
  #   $self->{'server'}->send_request_event({
  #     type => 'origin_host_max_confidence',
  #     pool => $pool,
  #     host => $host,
  #   });
  # }

  return $new_confidence;
}

sub event_origin_host_min_confidence {
  my($self,$event) = @_;
  my $pool = $event->{'pool'};
  my $host = $event->{'host'};

  # log this important event
  $self->{'server'}->s_log(1,
    "confidence: host $host (pool $pool) at minimum confidence");
}


sub _attempt_origin_connect {
  my($self, $request) = @_;
  return undef if $request->{'origin_connect_pending'};
  return undef if $request->{'origin_connection'};
  my $config = $self->{'server'}->{'config'}->{'forwarder'};  

  # if forwarder is in maintenance mode, skip connect attempts and fail out
  if($config->{'maintenance_mode'}) {
    $self->{'server'}->send_request_event({
      type    => 'origin_connect_maintenance_mode',
      request => $request,
    });
    return -1;
  }


  my($connect_to,$pool) = $self->select_origin_server_for_request($request);
  return undef unless defined $connect_to;
  $request->{'origin_connect_pending'} = 1;
  $self->{'server'}->s_log(2,"attempt_origin_connect: $pool,$connect_to");
  my $origin_connection = MCP::Poller::Connection::Origin->new({
    poller            => $self->{'server'}->{'poller'},
    request           => $request,
    connect_to        => $connect_to,
    pool              => $pool,
  });

  # remember when we started this connection
  $self->{'pending_connections'}->{scalar $origin_connection} =
    $origin_connection;
  $self->{'pending_connection_times'}->{scalar $origin_connection} = 
    scalar(time());
  $request->{'origin_connect_attempts'}++;
  return 1;
}

sub select_origin_server_for_request {
  my($self,$request) = @_;

  my ($pool_name, $pool_config) = $self->select_pool_for_request($request);
  return (undef,undef) unless defined $pool_name;

  # make sure the pool has been initialized before we choose a host
  # (this can happen when we specify an alternate_forwarder_pool for
  # the request, because the pool will have been constructed on the
  # fly and none of the initialization functions will have been called
  # for the pool yet)
  #
  unless($self->{'pool_dice'}->{$pool_name}) {
	  $self->_initialize_pool_state({ origin_pools => { $pool_name => $pool_config } });
  }

  return ($self->{'pool_dice'}->{$pool_name}->roll(),$pool_name);
}

sub select_pool_for_request {
  my($self,$request) = @_;
  return undef unless(ref($request) eq 'MCP::Request');
  my $config = $self->{'server'}->{'config'}->{'forwarder'};

  # if an explicit pool was supplied, use it.
  if(my $alt_config = $request->{'alternate_forwarder_pool'}) {
      my $pool_name = (keys(%{$alt_config}))[0];
      my $pool_config = $alt_config->{$pool_name};
      return ($pool_name, $pool_config);
  }

  my $host_path = $request->get_host_path();
  my $pool_name = undef;
  foreach my $matchspec (@{$config->{'pool_match'}}) {
    my($test_re,$pool) = @{$matchspec};
    if ($host_path =~ /$test_re/) {
	    $pool_name = $pool;
	    last;
    }
  }

  # if nothing matched, use default pool
  $pool_name ||= $config->{'default_origin_pool'};

  # stash the name of the pool into the request object
  $request->{'origin_pool'} = $pool_name;

  my $pool_config = $config->{'origin_pools'}->{$pool_name};
  return ($pool_name, $pool_config);
}

sub get_status_page {
  my($self,$request) = @_;
  my $config = $self->{'server'}->{'config'}->{'forwarder'};

  my $status = [];

  my $host_state_tree = {
    type  => 'toggletree',
    title => 'Origin Pool State',
    data  => [],
  };
  my $pn = 0;
  foreach my $pool (sort keys %{$self->{'host_state'}}) {
    my $pool_total;
    map { $pool_total += $_ } values(%{$self->{'host_state'}->{$pool}});
    my $pool_avg = scalar(keys(%{$self->{'host_state'}->{$pool}}))?
      ($pool_total/scalar(keys(%{$self->{'host_state'}->{$pool}})) ): 0;
    my $host_tree = [];
    my $hn = 0;
    foreach my $host (sort keys %{$self->{'host_state'}->{$pool}}) {
      my $conf = $self->{'host_state'}->{$pool}->{$host};
      push(@{$host_tree}, ["P$pn"."H$hn", 
        [ { type    => 'percentbar', 
            text    => int($conf*100)."%  $host",
            percent => int($conf*100) } ],
        undef]);
      $hn++;
    }
    push(@{$host_state_tree->{'data'}}, ["P$pn", 
      [ { type    => 'percentbar',
          text    => int($pool_avg*100)."%  $pool", 
          percent => int($pool_avg*100) } ],
      $host_tree]);
    $pn++;
  }
  push(@{$status}, $host_state_tree);

  my $config_table = {
    type  => 'table',
    title => 'Pool Configuration',
    data  => [['Regular Expression:','Pool:']],
  };
  foreach my $match (@{$config->{'pool_match'}}) {
    push(@{$config_table->{'data'}}, $match);
  }
  push(@{$config_table->{'data'}},
    ['(Default)',$config->{'default_origin_pool'}]);
  push(@{$status}, $config_table);

  return $status;
}

sub event_origin_connect_maintenance_mode { 
  my($self,$event) = @_;
  my $config = $self->{'server'}->{'config'}->{'forwarder'};
  my $request = $event->{'request'};
  return undef unless(ref($request) eq 'MCP::Request');

  $request->send_response($config->{'origin_connect_maintenance_mode'});
  return -1;
}

1;
__END__


