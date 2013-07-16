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

package MCP::RequestModule::BlobForwarder;
use base 'MCP::RequestModule';
use CLBlob::Client;
use Config::MultiJSON;
use strict;

sub proto {
    my($caller,$args) = @_;
    return {
        %{ shift->SUPER::proto(@_) },
        clients => [],
    }
}

sub init {
    my $self = shift;
    my $config = $self->{'server'}->{'config'}->{'blob_forwarder'};

    for my $forwarder (0..$#{$config}) {
        my $jsonconf = {};
        for my $config_dir (@{$config->[$forwarder]->{'config_dirs'}}) {
            $jsonconf = Config::MultiJSON::load_dir($jsonconf, $config_dir);
        }
        for my $config_file (@{$config->[$forwarder]->{'config_files'}}) {
            $jsonconf = Config::MultiJSON::load_file($jsonconf, $config_file);
        }
        $self->{'clients'}->[$forwarder] = new CLBlob::Client($jsonconf);
    }
}

sub event_received_cache_miss {
    my($self,$event) = @_;
    my $config = $self->{'server'}->{'config'}->{'blob_forwarder'};
    my $request = $event->{'request'};
    my $host_path = $request->get_host_path();
    my $client;
    MATCH: for my $forwarder (0..$#{$config}) {
        for my $match (@{$config->[$forwarder]->{'match'}}) {
            if ($host_path =~ /$match/) {
                $client = $self->{'clients'}->[$forwarder];
                last MATCH;
            }
        }
    }
    return undef unless $client;

    my @components = split(/\//, $host_path);
    my $name = pop(@components);
    my $replicas;
    eval { $replicas = $client->replicas($name, 'encoded' => 1); };
    if ($@) {
        print STDERR "BlobForwarder: $@\n";
        return $self->_throw_404($request);
    }
    my $pool_name = 'blob_' . join('_', @$replicas);
    my $pool_config = [];
    for my $replica (@$replicas) {
        push(@$pool_config, $client->{'replicas'}->{$replica}->{'ip'} . ':' .
            $client->{'replicas'}->{$replica}->{'port'});
    }
    $request->{'origin_retry_bad_status'} = 1;
    $request->{'alternate_forwarder_pool'} = { $pool_name => $pool_config };
}

sub _throw_404 {
    my($self,$request) = @_;

    $request->send_response({
        status         => '404',
        status_text    => 'Not Found',
        content_length => 0,
    });
    return -1;
}

1;

__END__
