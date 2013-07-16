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

package MCP::ServerConfig;
use base 'MCP::Object';
use strict;

sub proto {
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    _config_filename => $args->{'filename'},
  }
}

sub init {
  my $self = shift;

  # slurp the config in, put it in $self
  my $config_hash = $self->file_to_hash( $self->{'_config_filename'} );
  map { $self->{$_} = $config_hash->{$_} } keys(%{$config_hash});
}

sub file_to_hash {
  my($self,$filename) = @_;

  # slurp in file into one big scalar
  open(FH,"<$filename") || die "could not open $filename for read: $!";
  my $contents = undef;
  { local $/ = undef; $contents = <FH>; }
  close(FH);
 
  my $hash = {};

  # look for included files, merge them into the main hash
  my @includes = ($contents =~ /\n\#include\s+(.*?)\s*\n/sgo);
  foreach my $include (@includes) {
    # if filename does not start with /, assume relative to main config
    unless($include =~ /^\//o) {
      my($path) = ($filename =~ /^(.*)\//o);
      $include = $path.'/'.$include;
    }
    my $include_hash = $self->file_to_hash($include);
    $self->merge_hash($hash,$include_hash);
  }

  # eval the file, merge the resulting hash into the main hash
  my $file_hash = {
    eval $contents
  };
  $self->merge_hash($hash,$file_hash);

  return $hash;
}

sub merge_hash {
  my($caller,$h0,$h1) = @_;

  foreach my $h1k (keys %{$h1}) {
    if(exists $h0->{$h1k} && (ref $h0->{$h1k} eq 'HASH')) {
      $caller->merge_hash($h0->{$h1k},$h1->{$h1k});
      next;
    }
    $h0->{$h1k} = $h1->{$h1k};
  }
}

1;
__END__

