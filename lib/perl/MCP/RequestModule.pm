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

package MCP::RequestModule;
use base 'MCP::Object';
use strict;

use constant REQUEST_MODULE_EVENTS => (qw/
    event_poll_tick

    event_client_connect
    event_client_idle_timeout
    event_client_disconnect
    event_received_client_rline
    event_received_client_headers
    event_received_client_data_chunk
    event_sent_client_data_chunk_throttled

    event_received_cache_miss
    event_received_cache_hit
    event_received_index_key_miss
    event_received_index_key_hit

    event_pending_origin_connect
    event_origin_connect 
    event_origin_disconnect
    event_received_origin_rline
    event_received_origin_headers
    event_received_origin_data_chunk
    event_origin_connect_fail
    event_origin_host_min_confidence
    event_origin_host_max_confidence
    event_origin_host_confidence_change

    event_failed_to_parse_request
    event_request_uri_too_long
    event_origin_connect_max_retries
    event_origin_connect_maintenance_mode
    event_failed_to_parse_response
    event_admin_port_required
    event_request_blocked
    event_request_throttled
    event_max_request_length_exceeded
    event_request_url_not_modified
/);


sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    'server' => $args->{'server'},
  }
}

sub request_module_events { return [ REQUEST_MODULE_EVENTS ]; }

sub get_status_page { }

sub poll_tick_1s { }

1;
__END__

