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

package MCP::RequestModule::Redirector;
use base 'MCP::RequestModule';
use strict;


sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    config => $args->{'server'}->{'config'}->{'redirector'},
  }
}

sub init { }

sub event_received_client_headers {
  my($self,$event) = @_;
  my $request = $event->{'request'};

  my $host_path = $request->get_host_path();
  my $cookies = $request->get_header_value('Cookie');

  # cookie redirects
  my $cookie_redirects = $self->{'config'}->{'cookie_redirects'};
  if(defined $cookies) {
    my $cookies = $self->_make_hash_cookies($cookies);
    foreach my $c_name (keys %{$cookie_redirects}) {
      my $cookie_value = $cookies->{$c_name};
      next unless defined $cookie_value;
      foreach my $rdr (@{$cookie_redirects->{$c_name}}) { 
        my($match_re,$s) = @{$rdr};
        next unless($host_path =~ /$match_re/);
        next unless $self->_send_redirect($request,$s,$cookie_value);
        return -1;
      }
    }
  }

  # cookie rewrites
  my $cookie_rewrites = $self->{'config'}->{'cookie_rewrites'};
  if(defined $cookies) {
    my $cookies = $self->_make_hash_cookies($cookies);
    foreach my $c_name (keys %{$cookie_rewrites}) {
      my $cookie_value = $cookies->{$c_name};
      next unless defined $cookie_value;
      foreach my $rdr (@{$cookie_rewrites->{$c_name}}) { 
        my($match_re,$s,$r) = @{$rdr};
        next unless($host_path =~ /$match_re/);
        my $orig_host_path = $host_path;
        $host_path =~ s/$s/$r/;
	$host_path =~ s/COOKIE/$cookie_value/;
    	next if($orig_host_path eq $host_path);
        my($host,$path) = ($host_path =~ /^(.*?)(\/.*)$/o);
        $request->set_header_value('Host', $host);
        $request->{'path'} = $path;
        $request->{'host_path'} = undef; # clear this so it gets regenerated
      }
    }
  }

  # referer redirects
  my $referer_redirects = $self->{'config'}->{'referer_redirects'};
  my $referer = $request->get_header_value('Referer');
  if(defined $referer) {
    foreach my $rdr (@{$referer_redirects}) {
      my($match_re,$s,$r) = @{$rdr};
      next unless($referer   =~ /$match_re/);
      next unless($host_path =~ /$s/);
      # if replace string is not defined, send a 404 and end it.
      if(!defined $r) {
          $request->send_response({
            status         => '404',
            status_text    => 'Not Found',
            content_length => 0,
          });
          return -1;
      }
      next unless $self->_send_redirect($request,$s,$r);
      return -1;
    }
  }

  # referer referer redirects
  my $referer_referer_redirects = $self->{'config'}->{'referer_referer_redirects'};
  if(defined $referer) {
    foreach my $rdr (@{$referer_referer_redirects}) {
      my($match_re,$s,$r) = @{$rdr};
      next unless($host_path =~ /$match_re/);
      next unless($referer   =~ /$s/);
      # if replace string is not defined, send a 404 and end it.
      if(!defined $r) {
          $request->send_response({
            status         => '404',
            status_text    => 'Not Found',
            content_length => 0,
          });
          return -1;
      }
      my $alt_url = $referer;
      $alt_url =~ s/^.*?\:\/\///;
      next unless $self->_send_redirect($request,$s,$r,$alt_url);
      return -1;
    }
  }

  # agent redirects
  my $agent_redirects = $self->{'config'}->{'agent_redirects'};
  my $agent = $request->get_header_value('User-Agent');
  if(defined $agent) {
    foreach my $rdr (@{$agent_redirects}) {
      my($match_re,$s,$r) = @{$rdr};
      next unless($agent   =~ /$match_re/);
      next unless($host_path =~ /$s/);
      # if replace string is not defined, send a 404 and end it.
      if(!defined $r) {
          $request->send_response({
            status         => '404',
            status_text    => 'Not Found',
            content_length => 0,
          });
          return -1;
      }
      next unless $self->_send_redirect($request,$s,$r);
      return -1;
    }
  }

  # permanent redirects ('moves')
  my $moves = $self->{'config'}->{'moves'};
  foreach my $move (@{$moves}) {
    my($match_re,$s,$r) = @{$move};
    next unless($host_path =~ /$match_re/);
    next unless $self->_send_permanent_redirect($request,$s,$r);
    return -1;
  }

  # simple redirects
  my $redirects = $self->{'config'}->{'redirects'};
  foreach my $rdr (@{$redirects}) {
    my($match_re,$s,$r) = @{$rdr};
    next unless($host_path =~ /$match_re/);
    # if replace string is not defined, send a 404 and end it.
    if(!defined $r) {
	  $request->send_response({
		status         => '404',
		status_text    => 'Not Found',
		content_length => 0,
	  });
	  return -1;
    }
    next unless $self->_send_redirect($request,$s,$r);
    return -1;
  }

  # simple rewrites
  my $rewrites = $self->{'config'}->{'rewrites'};
  foreach my $rdr (@{$rewrites}) {
    my($match_re,$s,$r) = @{$rdr};
    next unless($host_path =~ /$match_re/);
    my $orig_host_path = $host_path;
    $host_path =~ s/$s/$r/;
    next if($orig_host_path eq $host_path);
    my($host,$path) = ($host_path =~ /^(.*?)(\/.*)$/o);
    $request->set_header_value('Host', $host);
    $request->{'path'} = $path;
    $request->{'host_path'} = undef; # clear this so it gets regenerated
  }
}

sub _send_permanent_redirect {
  my($self,$request,$s,$r) = @_;
  my $client_connection = $request->{'client_connection'};

  # modify the url as specified
  my $host_path = $request->get_host_path();
  my $orig_host_path = $host_path;
  $host_path =~ s/$s/$r/;

  # skip redirecting if url stays the same
  return undef if($orig_host_path eq $host_path);

  # send a 301 response
  $request->send_response({
    status         => '301',
    status_text    => 'Moved Permanently',
    content_length => 0,
    headers        => { Location => "http://$host_path" },
  });
  return 1;
}

sub _send_redirect {
  my($self,$request,$s,$r,$alt_host_path) = @_;
  my $client_connection = $request->{'client_connection'};

  # modify the url as specified
  my $host_path = $alt_host_path || $request->get_host_path();
  my $orig_host_path = $host_path;
  $host_path =~ s/$s/$r/;

  # skip redirecting if url stays the same
  return undef if($orig_host_path eq $host_path);

  # send a 302 response
  $request->send_response({
    status         => '302',
    status_text    => 'Found',
    content_length => 0,
    headers        => { Location => "http://$host_path" },
  });
  return 1;
}

sub _make_hash_cookies {
  my($self,$cookies_hdrs) = @_;
  $cookies_hdrs = [ $cookies_hdrs ] unless ref $cookies_hdrs;
  my $cookies_header = join(';', @{$cookies_hdrs});

  return { split(/\s*[=;]\s*/o, $cookies_header) }; 
}


1;
__END__


