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

package MCP;
use MCP::Server::Proxy;
use MCP::ServerConfig;
use strict;


sub get_proxy_server { return shift->_get_server('Proxy',@_); }
#sub get_cache_server { return shift->_get_server('Cache',@_); }
#sub get_state_server { return shift->_get_server('State',@_); }


sub _get_server {
  my($caller,$type,$args) = @_;

  my $server_package = "MCP::Server::$type";
  my $server = $server_package->new({
    server_config => $args->{'config_ref'} || 
                     $caller->get_server_config($args->{'server_config'}),
    process_name  => $args->{'process_name'},
    log_level     => $args->{'log_level'},
    log_file      => $args->{'log_file'},
    syslog_ident  => $args->{'syslog_ident'},
  });

  return $server;
}

sub get_server_config {
  my($caller,$filename) = @_;
  return MCP::ServerConfig->new({
    filename => $filename,
  });
}


1;
__END__

