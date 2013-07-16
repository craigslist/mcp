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

package MCP::RequestModule::DirectCache;
use base 'MCP::RequestModule';
use MCP::Poller::Connection::Memcached;
use Time::Local;
use Data::Dumper;
use Cache::Memcached;
use Time::HiRes qw/ gettimeofday /;
use strict;

use constant DEBUG => 0;

use constant MAX_KEY_LENGTH => 220;
use constant MAX_HOT_CACHE_TTL_JITTER => 256;
use constant DEFAULT_HOT_CACHE_REFRESH => 10;

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    memcached              => undef,
    cache                  => {},   
    cache_timeout          => {},   
    pending_gets           => {},
    pending_get_count      => 0,
    gets_underway          => {},
    last_get_time          => 0,
    nonblocking            => 0,
    hot_cache              => {}, 
    ttl_jitter             => 0,
    last_hot_cache_refresh => scalar(time()),
  }
}

sub init {
  my($self,$args) = @_;

  # create a Cache::Memcached object, connecting to memcached servers
  $self->{'nonblocking'} = 1 
    if($self->{'server'}->{'config'}->{'cache_server'}->{'nonblocking'});
  my $servers = $self->_server_list_to_server_array(
    $self->{'server'}->{'config'}->{'cache_server'}->{'memcached_servers'}
  );
  my $memcached_args = {
    #TODO compress threshold?
    servers     => $servers,
    debug       => 0,
    no_rehash   => 0,
  };
  #TODO support blocking mode
  $self->{'memcached'} = $self->{'nonblocking'}?
    'MCP::Poller::Connection::Memcached':
    Cache::Memcached->new($memcached_args);
  $self->{'memcached'}->set_cache_args($memcached_args)
    if($self->{'nonblocking'});
}


sub event_received_client_rline {
  my($self, $event) = @_;
  my $request = $event->{'request'};

  # do this now, in case it breaks so we can issue exception faster
  $request->parse_rline() if ref $request;
}

sub event_received_client_headers {
  my($self, $event) = @_;
  my $request = $event->{'request'};
  my $config  = $self->{'server'}->{'config'}->{'cache_server'};

  #XXX TIMER BEGIN fixup
  $request->{'timers'}->{'fixup'} = gettimeofday();

  # prevent this connection from being timed out now that we have headers
  $request->{'client_connection'}->cancel_idle_timeout();

  # determine request method and proceed
  my($method,$path,$proto) = $request->parse_rline();
  my $read_methods = $config->{'cache_read_methods'};
  if($method =~ /($read_methods)/) {

    # check client If-Modified-Since header and blind_304 list
    if(my $ims = $request->get_header_value('If-Modified-Since')) {
      my $host_path = $request->get_host_path();
      if(ref($config->{'blind_304_match'}) eq 'ARRAY') {
        foreach my $ims_match (@{$config->{'blind_304_match'}}) {
          if($host_path =~ /$ims_match/) {
	        # FIXME: should this be reported as DirectCache rather than Exceptions?
            $self->{'server'}->send_request_event({
              type    => 'request_url_not_modified',
              request => $request,
            });

            #XXX TIMER END fixup
            $request->{'timers'}->{'fixup'} = 
              gettimeofday() - $request->{'timers'}->{'fixup'};
            return -1;
          }
        }
      }
    }
   
    # go look for this url in the cache
    my $cache_key = $self->_get_cache_key_for_request($request);
    if(exists $self->{'hot_cache'}->{$cache_key}) {
      $self->{'server'}->send_request_event({
        type      => 'received_cache_hit',
        request   => $request,
        key       => $cache_key,
        hit_value => \$self->{'hot_cache'}->{$cache_key},
        no_cache => 1,
      });
    }
    else {
      $self->enqueue_cache_get($cache_key,$request);
    }
    return -1;
  }
  elsif($method eq 'PURGE') {
    # purges only allowed on admin ports
    unless($request->{'client_connection'}->{'is_admin'}) {
      $self->{'server'}->send_request_event({
        type    => 'admin_port_required',
        request => $request,
      });
      return -1;
    }

    # if Content-Length is nonzero, this is a multi-page purge
    if(my $len = $request->get_header_value('Content-Length')) {
      $request->{'purge_content_length'} = $len;
    }

    # enqueue delete request for primary url
    foreach my $cache_key ($self->_get_common_keys_for_request($request,1)) {
      $self->{'memcached'}->enqueue_delete(undef,sub { }, $cache_key);
    }

    $self->_prepare_purge_response($request);
    return -1;
  }
 
  return undef;
}

sub event_received_client_data_chunk {
  use bytes;
  my($self,$event) = @_;
  my $request = $event->{'request'};

  # if we're waiting on content for a multi-page purge, buffer it in request
  if(my $len = $request->{'purge_content_length'}) {
    my $client_connection = $request->{'client_connection'};
    $request->{'purge_content'} .= $client_connection->{'raw_content'};
    $client_connection->{'raw_content'} = '';

    # if we have the whole thing, decode it, issue deletes
    if(length($request->{'purge_content'}) >= $len) {
      foreach my $hostpath (split(/\s*\n\s*/so, $request->{'purge_content'})) {
        foreach my $k ($self->_get_common_keys_for_hostpath($hostpath)) {
          $self->{'memcached'}->enqueue_delete(undef,sub { }, $k);
        }
      }
    }

    # send a response if we're all done
    $self->_prepare_purge_response($request);
    return -1;
  }
}

sub event_poll_tick {
  my($self,$event) = @_;
  my $config  = $self->{'server'}->{'config'}->{'cache_server'};

  # if get queue timeout has elapesed, do the cache gets now
  if(($event->{'time'} - $self->{'last_get_time'}) >= 
      $config->{'get_queue_timeout'}) {
    $self->{'last_get_time'} = $event->{'time'};
    $self->do_cache_gets();
  }
}

sub poll_tick_1s {
  my($self,$now) = @_;
  return undef unless ref $self;
  my $config  = $self->{'server'}->{'config'}->{'cache_server'};

  # refresh the hot cache every N - 2N seconds
  my $refresh_interval = 
      $config->{'hot_cache_refresh'} || DEFAULT_HOT_CACHE_REFRESH;
  $refresh_interval += ($self->{'ttl_jitter'} % $refresh_interval);
  if(($now - $refresh_interval) >= $self->{'last_hot_cache_refresh'}) {
      $self->{'hot_cache'} = {};
      $self->{'last_hot_cache_refresh'} = $now;
  }

  foreach my $request_id (keys(%{$self->{'cache'}})) {
    if(exists $self->{'cache_timeout'}->{$request_id}) {
      delete $self->{'cache_timeout'}->{$request_id};
      delete $self->{'cache'}->{$request_id};
    }
    else {
      $self->{'cache_timeout'}->{$request_id} = $now;
    }
  }
  foreach my $request_id (keys(%{$self->{'cache_timeout'}})) {
    delete $self->{'cache_timeout'}->{$request_id}
      unless exists $self->{'cache'}->{$request_id};
  }
}

sub enqueue_cache_get {
  my($self,$cache_key,$request,$event_type) = @_;
  my $config  = $self->{'server'}->{'config'}->{'cache_server'};
  $event_type ||= 'cache';

  print STDERR "enqueue_cache_get"
    . " type " . ($event_type || 'undef')
      . " key " . ($cache_key || 'undef')
        . "\n" if DEBUG;

  # put this get on the pending list
  $self->{'pending_gets'}->{$cache_key} ||= [];
  push(@{$self->{'pending_gets'}->{$cache_key}}, [$request,$event_type]);
  $self->{'pending_get_count'}++;

  # do a cache get right now if the queue is full or if we're not busy lately
  my $now = gettimeofday();
  if($self->{'pending_get_count'} >= $config->{'max_get_queue_size'}) {
    $self->{'last_get_time'} = $now;
    $self->do_cache_gets();
  }
}

sub do_cache_gets {
  my $self = shift;

  # get all the keys in the pending list from memcached
  my @cache_keys = keys(%{$self->{'pending_gets'}});
  return undef unless scalar @cache_keys;

  my $callback = sub {
    my($response,$k,$v) = @_;

    return unless exists $self->{'gets_underway'}->{$k};

    # iterate through results, issue hit/miss events
    my $total = 0;
    if($response eq 'HIT') {
      foreach my $req_evt (@{$self->{'gets_underway'}->{$k}}) {
	      print STDERR "hit k " . ($k || 'undef') . "\n" if DEBUG;
        my($request,$event_type) = @{$req_evt};
        $self->{'server'}->send_request_event({
          type      => 'received_'.$event_type.'_hit',
          request   => $request,
          key       => $k,
          hit_value => $v,
        });
        $total++;
      }
    }
    elsif($response eq 'MISS') {
      foreach my $req_evt (@{$self->{'gets_underway'}->{$k}}) {
        my($request,$event_type) = @{$req_evt};
        $self->{'server'}->send_request_event({
          type    => 'received_'.$event_type.'_miss',
          key     => $k,
          request => $request,
        });
        $total++;
      }
    }
    else {
      print STDERR "unknown callback response type '$response'\n";
    }

    delete $self->{'gets_underway'}->{$k};
    return;
  };

  $self->{'memcached'}->enqueue_get(undef,$callback,@cache_keys);

  # reset get queue and counters
  foreach my $k (@cache_keys) {
    $self->{'gets_underway'}->{$k} ||= [];
    push(@{$self->{'gets_underway'}->{$k}}, @{$self->{'pending_gets'}->{$k}});
    delete $self->{'pending_gets'}->{$k};
  }
  $self->{'pending_get_count'} -= scalar(@cache_keys);
}

sub event_received_cache_hit {
  use bytes;
  my($self,$event) = @_;
  my $request           = $event->{'request'};
  my $client_connection = $request->{'client_connection'};
  my $config  = $self->{'server'}->{'config'}->{'cache_server'};
  my $fwd_config  = $self->{'server'}->{'config'}->{'forwarder'};

	my $hit_value = ${$event->{'hit_value'}};
  my $cache_key = $event->{'key'};
  # store also in hot cache if this url matches
  if((ref($config->{'hot_cache_match'}) eq 'ARRAY') && !$event->{'no_cache'}) {
    my $host_path = $request->get_host_path();
    foreach my $hot_match (@{$config->{'hot_cache_match'}}) {
      if($host_path =~ /$hot_match/) {
        my $hot_hit_value = $hit_value;
        my($hit_expires) = unpack('I',substr($hot_hit_value, 0, 4, ''));
        $self->{'ttl_jitter'} ||= int(rand(MAX_HOT_CACHE_TTL_JITTER));
        $hit_expires += $self->{'ttl_jitter'};
        $self->{'hot_cache'}->{$cache_key} =  pack('I',$hit_expires).$hot_hit_value;
      }
    }
  }

  # unpack and examine expire time, if ttl is too old, issue cache_miss event
  # also, if the forwarder is in maintenance mode, serve old stuff no matter
  my($hit_expires) = unpack('I',substr($hit_value, 0, 4, ''));
  if($hit_expires < scalar(time())) {
      $request->{'stale_cache_hit'} = $hit_value;
      unless($fwd_config && $fwd_config->{'maintenance_mode'}) {
          $self->{'server'}->send_request_event({
              type    => 'received_cache_miss',
              request => $request,
          });
	  return -1;
      }
  }

  #XXX TIMER END fixup
  $request->{'timers'}->{'fixup'} = 
    gettimeofday() - $request->{'timers'}->{'fixup'};

  # add extra headers if needed
  if(scalar(keys(%{$self->{'extra_response_headers'}}))) {
      my $extra_head = join("\r\n", map {
          $_.': '.$self->{'extra_response_headers'}->{$_}
      } keys(%{$self->{'extra_response_headers'}}) );
      $hit_value = join($extra_head, split(/\r\n/, $hit_value));
  }

  # yay!  cache hit...send the data along, register, and disconnect
  $client_connection->write_data( $hit_value );
  $request->register_response({
    status         => '200',
    status_text    => 'OK',
    content_length => length($hit_value),
  });
  $client_connection->close();
  return -1;
}

sub _prepare_purge_response {
  use bytes;
  my($self,$request) = @_;

  # exit here if we're still anticipating multi-page purge content from client
  return  
    if(length($request->{'purge_content'}) <
       $request->{'purge_content_length'});

  # if we're all out of pages to purge, return a response
  my $purged   = $request->{'purged_pages'};
  my $unpurged = $request->{'unpurged_pages'};
  my $content = $purged.'/'.($purged+$unpurged)." pages purged\n";
  $request->send_response({
    status      => '200',
    status_text => 'OK',
    content     => $content,
  });
}

sub event_received_origin_rline {
  use bytes;
  my($self, $event) = @_;
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};
  my $client_connection = $request->{'client_connection'};

  # if response from origin server is 304, try stale cache fetch
  if($request->get_response_status() eq '304') {
    if(length($request->{'stale_cache_hit'})) {
      $client_connection->write_data( $request->{'stale_cache_hit'} );
      $request->register_response({
        status         => '200',
        status_text    => 'OK',
        content_length => length($request->{'stale_cache_hit'}),
      });
      $client_connection->close();
      return -1;
    }
  }

  # prepare to store this in the cache
  my $request_id = $request->{'id'};
  delete $self->{'cache_timeout'}->{$request_id}; 
  $self->{'cache'}->{$request_id} = $request->get_response_rline()."\r\n"; 
}

sub event_received_origin_headers {
  my($self, $event) = @_;
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};

 
  # store header string temporarily in case we want to cache it later
  my $request_id = $request->{'id'};
  return undef unless $self->{'cache'}->{$request_id};
  delete $self->{'cache_timeout'}->{$request_id}; 
  $self->{'cache'}->{$request_id} .= 
    $request->get_response_header_string(1)."\r\n\r\n"; 
}

sub event_received_origin_data_chunk {
  my($self, $event) = @_;
  my $request           = $event->{'request'};
  my $origin_connection = $event->{'connection'};

  # store response contente temporarily in case we want to cache it later
  my $request_id = $request->{'id'};
  return undef unless $self->{'cache'}->{$request_id};
  delete $self->{'cache_timeout'}->{$request_id}; 
  $self->{'cache'}->{$request_id} .= $origin_connection->{'raw_content'};
}

sub event_origin_disconnect {
  use bytes;
  my($self, $event) = @_;
  my $config  = $self->{'server'}->{'config'}->{'cache_server'};
  my $request = $event->{'request'};

  my $request_id = $request->{'id'};
  my $cache_key = $self->_get_cache_key_for_response($request);
  return undef unless $self->{'cache'}->{$request_id};
  unless($cache_key) {
    delete $self->{'cache_timeout'}->{$request_id};
    delete $self->{'cache'}->{$request_id};
    return undef;
  }

  # store this page in the shared cache if it is cacheable
  if($self->_should_cache_response($request)) {

    # store page key in the cache
    my $ttl = $self->_get_response_ttl($request);
    my $expires = scalar(time()) + $ttl;
    if($ttl > 0) {
	    print STDERR "store key " . ($cache_key || 'undef') . " ttl " . ($ttl || 'undef') . "\n" if DEBUG;
      $self->{'memcached'}->enqueue_set(undef,sub { }, 
        $cache_key, 
        pack('I',$expires).$self->{'cache'}->{$request_id}
      );
      $self->{'server'}->s_log(2,"cache_store: $cache_key (".
        length($self->{'cache'}->{$request_id})." bytes, ttl: $ttl)");
    }

    # store also in hot cache if this url matches
    if(ref($config->{'hot_cache_match'}) eq 'ARRAY') {
      my $host_path = $request->get_host_path();
      foreach my $hot_match (@{$config->{'hot_cache_match'}}) {
        if($host_path =~ /$hot_match/) {
          $self->{'ttl_jitter'} ||= int(rand(MAX_HOT_CACHE_TTL_JITTER));
          $expires += $self->{'ttl_jitter'};
          $self->{'hot_cache'}->{$cache_key} = 
            pack('I',$expires).$self->{'cache'}->{$request_id};
        }
      }
    }

    # clean up temporary cache buffer
    delete $self->{'cache'}->{$request_id};
  }
  else {
    delete $self->{'cache_timeout'}->{$request_id};
    delete $self->{'cache'}->{$request_id};
  }

}


sub _should_cache_response {
  my($self,$request) = @_;
  my $origin_connection = $request->{'origin_connection'};
  my $config = $self->{'server'}->{'config'}->{'cache_server'};

  # don't cache anything except allowed responses (usually GET / 200 only)
  my($method,$path,$proto) = $request->parse_rline();
  my $write_methods = $config->{'cache_write_methods'};
  return undef unless($method =~ /($write_methods)/);

  # don't cache anything except 200 OK responses, unless so exempted
  unless($request->get_response_status() eq '200') {
    return undef unless($request->get_response_status() eq '404');
    my $cache_404 = 0;
    my $host_path = $request->get_host_path();
    if(ref $config->{'cache_404_match'} eq 'ARRAY') {
      foreach my $match_re (@{$config->{'cache_404_match'}}) {
        $cache_404 = 1 if($host_path =~ /$match_re/);
      }
    }
    return undef unless $cache_404;
  }

  # examine Vary: header for * to determine cachability
  my $vary_header = $request->get_response_header_value('Vary');
  return undef if(defined $vary_header && ($vary_header =~ /\*/o));

  # obey Cache-Control header
  my $cache_control = $request->get_response_header_value('Cache-Control');
  return 1 if($cache_control =~ /public/o);       # always cache
  return undef if($cache_control =~ /private/o);  # never cache
  return undef if($cache_control =~ /no-cache/o); # never cache
  return undef if($cache_control =~ /no-store/o); # never cache

  # cache if an expire time is specified
  my $expires_header = $request->get_response_header_value('Expires');
  return 1 if $expires_header;

  # otherwise, don't cache
  return undef;
}

sub _get_response_ttl {
  my($self,$request) = @_;
  my $config = $self->{'server'}->{'config'}->{'cache_server'};
  my $now = scalar(time());

  # use Cache-Control and Expires headers to determine this
  # let special X-MCP-Cache-Control header trump Cache-Control
  my $cache_control = $request->get_response_header_value('X-MCP-Cache-Control') || 
                      $request->get_response_header_value('Cache-Control');
  my $expires       = $request->get_response_header_value('Expires');

  # Cache-Control settings override Expires header
  my $ttl = $config->{'default_page_expiration'};
  if($expires) {
    # make sure the expires header is parseable; _expires_header_to_epoch
	# will return undef if we couldn't parse the string
	if (my $expires_epoch = $self->_expires_header_to_epoch($expires)) {
      $ttl = $expires_epoch - $now;
    }
  }
  if($cache_control) {
    my($max_age) = ($cache_control =~ /max-age\s*\=\s*(\d+)/o);
    $ttl = $max_age if $max_age;
  }

  return $ttl;
}

sub _get_cache_key_for_response { shift->_get_cache_key_for_request(@_,1) }

sub _get_cache_key_for_request {
  use bytes;
  my($self,$request,$for_response) = @_;
  return undef unless(ref($request) eq 'MCP::Request');

  my $config = $self->{'server'}->{'config'}->{'cache_server'};

  my $host_path = $request->{'cache_host_path'} || $request->get_host_path();
  my $proto     = $request->{'proto'};
  my $zip       = $request->wants_gzip();
  my $vary      = ''; #TODO support Vary: header one day
  my $cache_key = join(':','page',$proto,$zip,$vary,$host_path);

  # whitespace no good in cache keys
  $cache_key =~ s/\s//sgo;

  $cache_key = substr($cache_key,0,MAX_KEY_LENGTH)
    if(length($cache_key) > MAX_KEY_LENGTH);

  # perform key transformations if appropriate
  if(ref($config->{'cache_key_transformations'}) eq 'ARRAY') {
	  my $host_path = $request->get_host_path();
	  foreach my $elem (@{$config->{'cache_key_transformations'}}) {
		  my ($match, $thunk) = @$elem;
		  if($host_path =~ /$match/) {
			  $cache_key = &$thunk($request, $cache_key);
		  }
	  }
  }

  return $cache_key;
}

sub _get_common_keys_for_request {
  my($self,$request,$useport) = @_;
  
  my $host_path = $request->get_host_path($useport);
  # FIXME: $host_path will not be correct for https purges, since it
  # will strip the :443 part of the hostname, and therefore not clear
  # the correct thing out of the cache.

  return $self->_get_common_keys_for_hostpath($host_path);
}

sub _get_common_keys_for_hostpath {
  use bytes;
  my ($self,$host_path) = @_;
 
  my @common_keys;
  foreach my $proto ('1.0','1.1') {
    foreach my $zip (0,1) {
      my $cache_key = join(':','page',$proto,$zip,'',$host_path);
      $cache_key =~ s/\s//sgo;

      $cache_key = substr($cache_key,0,MAX_KEY_LENGTH)
        if(length($cache_key) > MAX_KEY_LENGTH);

      push @common_keys, $cache_key;
    }
  }

  return @common_keys;
}

sub _server_list_to_server_array {
  my($caller,$value) = @_;

  my $ar = [];

  foreach my $server (split(/\;/, $value)) {
    $server = [ split(/\,/, $server) ] if($server =~ /\,/);
    push @{$ar}, $server;
  }

  return $ar;
}

sub _expires_header_to_epoch {
  my($self,$expires) = @_;

  # return epoch time by parsing expires compliant with RFC2616 3.3.1

  # return undef if we could not parse the date

  # return jan 1 2038 if the year >= 2038 so we don't overflow the 32
  # bits that timegm wants to return

  my($wday,$mday,$mon,$yr,$hh,$mm,$ss,$tz);
  if($expires =~ /\,/o) {
    ($wday,$mday,$mon,$yr,$hh,$mm,$ss,$tz) = split(/[,\s:-]+/o,$expires);
  }
  else {
    ($wday,$mon,$mday,$hh,$mm,$ss,$yr) = split(/[,\s:-]+/o,$expires);
  }
  $yr += 2000 if($yr < 100);
  my $months = {qw/ jan 0 feb 1 mar 2 apr 3 may 4 jun 5 jul 6 
                    aug 7 sep 8 oct 9 nov 10 dec 11 /};
  $mon = lc($mon);
  return undef unless exists $months->{$mon};
  $mon = $months->{$mon};

  # big year? return jan 1 2038
  return timegm(0,0,0,1,0,2038) if ($yr >= 2038);

  # try running timegm; if it croaks, return undef, otherwise return
  # the result
  my $epoch;
  eval { $epoch = timegm($ss,$mm,$hh,$mday,$mon,$yr); };
  if ($@) {
	  print STDERR "_expires_header_to_epoch: $expires: $@\n" if DEBUG;
	  return undef;
  }
  return $epoch;
}

sub get_status_page {
  my($self,$request) = @_;
  my $config = $self->{'server'}->{'config'}->{'cache_server'};

  my $status = [];

  my $pending_req = $self->{'memcached'}->get_active_request_info();
  my $stats_table = {
    type  => 'table',
    title => 'Cache Statistics',
    data  => [],
  };
  push(@{$stats_table->{'data'}},
    [ 'Spooled Cache Gets:', $self->{'pending_get_count'} ],
    [ 'Buffered Cache Sets:', scalar(keys %{$self->{'cache'}}) ],
    [ 'Pending Cache Gets:', scalar(@{$pending_req->{'GET'}})],
    [ 'Pending Cache Sets:', scalar(@{$pending_req->{'SET'}})],
    [ 'Pending Cache Deletes:', scalar(@{$pending_req->{'DELETE'}})],
  );
  push(@{$status}, $stats_table);

  if($config->{'nonblocking'}) {
    my @cache_socket_info = $self->{'memcached'}->get_cache_socket_info();
    my $socket_table = {
      type  => 'table',
      title => 'Cache Sockets',
      data  => [],
    };
    push(@{$socket_table->{'data'}},
      [ 'Connected to:','Connection Status:',
        'Request Queue Length:','Poller Watch:',
        'Current Request Type:','Previous Request Type:' ],
      @cache_socket_info,
    );
    push(@{$status}, $socket_table);
  }

  my $config_table = {
    type  => 'table',
    title => 'Cache Configuration',
    data  => [],
  };
  my $cache_servers = $config->{'memcached_servers'};
  $cache_servers =~ s/\;/<br>/g;
  push(@{$config_table->{'data'}},
    [ 'Default Page Expiration:', $config->{'default_page_expiration'}.' s' ],
    [ 'Nonblocking Mode:', $config->{'nonblocking'}? 'Yes': 'No' ],
    [ 'Get Queue Max Size:', $config->{'max_get_queue_size'} ],
    [ 'Get Queue Timeout:', $config->{'get_queue_timeout'}.' s' ],
    [ 'Memcached Servers:', $cache_servers ],
  );
  push(@{$status}, $config_table);

  return $status;
}



1;
__END__


