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

package MCP::RequestModule::StatusPage;
use base 'MCP::RequestModule';
use MCP::Request;
use strict;


sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    config    => $args->{'server'}->{'config'}->{'status_page'},
    init_time => scalar(time()),
  }
}

sub init { }

sub event_received_client_headers {
  my($self,$event) = @_;
  my $request = $event->{'request'};

  # send a status page if the requested url matches status page url config 
  my $host_path = $request->get_host_path();
  my $status_url_match = $self->{'config'}->{'url_match'};
  return undef unless $status_url_match;
  if($host_path =~ /$status_url_match/) {
    $self->_send_status_page($request);
    return -1;
  }
}

sub get_status_page {
  my($self,$request) = @_;

  my $status = [
    { type  => 'table',
      title => 'Server Statistics',
      data  => [
        [ 'host:', `hostname` ],
        [ 'process:', $self->{'server'}->{'process_name'} ],
        [ 'pid:', $$ ],
        [ 'uptime:', $self->s2timespan(scalar(time())-$self->{'init_time'}) ],
        [ 'listeners:', scalar(keys %{$self->{'server'}->{'listeners'}}) ],
        [ 'total connections:', 
          scalar(keys %{$self->{'server'}->{'poller'}->{'connections'}}) ],
        [ 'requests active:', MCP::Request->num_active_requests() ],
        [ 'requests served:', MCP::Request->num_served_requests() ],
      ],
    },
    { type  => 'table',
      title => 'Active Requests',
      data  => [
        [ 'ID:', 'Request URI:', 'Duration:','Response Status:',
          'Last Request Event:',
          'Client Bytes Read:','Client Bytes Written:','Client IP:',
          'Origin Bytes Read:','Origin Bytes Written:','Origin IP:' ],
        map { [ 
          '<small>'.$_->{'id'}.'</small>', '<small>HTTP/'.$_->{'proto'}.
          ' <b>'.$_->{'method'}.'</b></small><br>'. $_->get_host_path(), 
          int($_->get_duration() * 1000).'ms', $_->{'response_status'}, 
          $_->{'last_request_event'}, 
          $self->prettybytes($_->{'client_connection'}->{'bytes_read'}), 
          $self->prettybytes($_->{'client_connection'}->{'bytes_written'}), 
          $_->{'client_connection'}->{'remote_ip'}, 
          ($_->{'origin_connection'}? 
            $self->prettybytes($_->{'origin_connection'}->{'bytes_read'}):''),
          ($_->{'origin_connection'}? 
            $self->prettybytes($_->{'origin_connection'}->{'bytes_written'}):''),
          ($_->{'origin_connection'}? 
            $_->{'origin_connection'}->{'connect_to'}: ''),
        ] } values(%{MCP::Request->get_active_requests()}),
      ],
    },
  ];

  return $status;
}

sub s2timespan {
  my($caller,$s) = @_;

  my $text = '';
  if($s >= 86400) {
    my $d = int($s / 86400);
    $s -= ($d * 86400);
    $text .= $d.'d ';
  }
  if($s >= 3600) {
    my $h = int($s / 3600);
    $s -= ($h * 3600);
    $text .= $h.'h ';
  }
  if($s >= 60) {
    my $m = int($s / 60);
    $s -= ($m * 60);
    $text .= $m.'min ';
  }
  $text .= $s.'s' if($s > 0);

  return $text;
}

sub prettybytes {
  my($caller,$b) = @_;

  if($b > (1024 * 1024 * 1024 * 1024)) {
    my $tb = $b / (1024 * 1024 * 1024 * 1024);
    return (int($tb * 100) / 100).'TB';
  }
  if($b > (1024 * 1024 * 1024)) {
    my $gb = $b / (1024 * 1024 * 1024);
    return (int($gb * 100) / 100).'GB';
  }
  if($b > (1024 * 1024)) {
    my $mb = $b / (1024 * 1024);
    return (int($mb * 100) / 100).'MB';
  }
  if($b > (1024)) {
    my $kb = $b / (1024);
    return (int($kb * 100) / 100).'KB';
  }
  return $b.'B';
}


sub _send_status_page {
  my($self,$request) = @_;
  my $config = $self->{'config'};
  my $server_config = $self->{'server'}->{'config'};

  # build module status hash from each request module
  my $module_status = {};
  my $request_module_sequence = $server_config->{'request_module_sequence'};
  foreach my $module_name (@{$request_module_sequence}) {
    my $module = $self->{'server'}->{'request_modules'}->{$module_name};
    my $status = $module->get_status_page($request);
    $module_status->{$module_name} = $status
      if ref $status;
  }

  # convert module status hash into html content
  my $content = $self->_module_status_to_html($module_status);

  # send response
  $request->send_response({
    status      => '200',
    status_text => 'OK',
    headers     => {
      'Cache-Control' => 'private',
    },
    content     => $content,
  });
}

sub _module_status_to_html {
  my($self,$module_status) = @_;
  my $config = $self->{'config'};
  my $server_config = $self->{'server'}->{'config'};
  my $request_module_sequence = $server_config->{'request_module_sequence'};

  # build header
  my $content = $config->{'page_header'};
 
  # build module list
  $content .= '<div class="module_list">';
  foreach my $module_name (@{$request_module_sequence}) {
    next unless exists $module_status->{$module_name};
    $content .= '<span class="module_list_item" id="'.$module_name.'">'.
      '<a href="javascript:toggleExclusive(\'module_'.$module_name.'\');">'.
      $module_name.'</a></span>';
  } 
  $content .= '</div>';

  $content .= '<div class="module_status_outer">';
  # build status div for each module
  foreach my $module_name (@{$request_module_sequence}) {
    next unless exists $module_status->{$module_name};
    $content .= '<div class="module_status" id="module_'.$module_name.'">';
    $content .= $self->_status_items_to_html($module_status->{$module_name});
    $content .= '</div>';
  }
  $content .= '</div>';

  # add footer and return
  $content .= $config->{'page_footer'};
  return $content;
}

sub _status_items_to_html {
  my($self,$items) = @_;

  my $content = '';
  foreach my $item (@{$items}) {
    my $html_sub = '_module_status_'.$item->{'type'}.'_to_html';
    $content .= $self->$html_sub($item); 
  }
  return $content;
}

sub _module_status_table_to_html {
  my($self,$item) = @_;
  my $data = $item->{'data'};

  my $content = '';
  $content .= '<h2>'.$item->{'title'}.'</h2>' if($item->{'title'});
  $content .= '<table>';
  foreach my $row (@{$data}) {
    $content .= '<tr>';
    foreach my $col_val (@{$row}) {
      $col_val = $self->_status_items_to_html($col_val)
        if(ref($col_val) eq 'ARRAY');
      $content .= '<td>'.$col_val.'</td>'; 
    }
    $content .= '</tr>';
  }
  $content .= '</table><br><br>';
  return $content;
}

sub _module_status_percentbar_to_html {
  my($self,$item) = @_;
  my $p = int($item->{'percent'});
  my $np = 100 - $p;

  my $bgc = '#bfb';  my $boc = '#0b0';
  if($p < 70) {
    $bgc = '#ffb';  $boc = '#bb0';
  }
  if($p < 30) {
    $bgc = '#fbb';  $boc = '#b00';
  }
  my $c = "background-color: $bgc; border: 1px solid $boc;";

  my $content = '<span class="percentbar">'.
    '<span class="percentbar_p" style="padding-left: '.$p.'px;'.$c.'"></span>'.
    '<span class="percentbar_np" style="padding-left: '.$np.'px"></span>'.
    '</span><span class="percentbar_text">'.
    $item->{'text'}.
    '</span>';
  return $content; 
}

sub _module_status_toggletree_to_html {
  my($self,$item) = @_;
  my $data = $item->{'data'};

  my $content = '';
  $content .= '<h2>'.$item->{'title'}.'</h2>' if($item->{'title'});
  $content .= $self->_toggletree_branch($data);
  return $content.'<br><br>';
}

sub _module_status_text_to_html {
  my($self,$item) = @_;
  my $data = $item->{'data'};
  my $content = '';
  $content .= '<pre>'.$data.'</pre>' if $data;
  return $content;
}

sub _toggletree_branch {
  my($self,$data) = @_;

  my $content = '';
  foreach my $item (@{$data}) {
    my($key,$text,$subdata) = @{$item};
    $content .= '<div class="toggletree" id="toggletree_'.$key.'">';
    $text = $self->_status_items_to_html($text)
      if((ref($text) eq 'ARRAY') && (ref($text->[0]) eq 'HASH'));
    if(ref $subdata) {
      $content .= 
        '<a href="javascript:toggle(\'toggletreelist_'.$key.'\')">'.
        '[*]</a>'.$text.'<br>'.
        '<div class="toggletree_list" id="toggletreelist_'.$key.'">'.
        $self->_toggletree_branch($subdata,$key).'</div>';
    }
    else {
      $content .= $text;
    }
    $content .= '</div>';
  }

  return $content;
}



1;
__END__


