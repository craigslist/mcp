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

package MCP::WeightedDice;
use strict;


sub new {
  my($caller,$weights) = @_;

  my $self = bless { weights => {}, high_lim => {}, low_lim => {} }, $caller;

  $self->adjust($weights) if ref $weights;

  return $self;
}


sub adjust {
  my($self, $weights) = @_;
  return undef unless ref $self;

  # get weight total
  my $weight_total = 0;
  foreach my $weight (values %{$weights}) {
    $weight_total += $weight;
  }
  $self->{'weight_total'} = $weight_total;
  $self->{'weights'} = $weights;

  # build high/low limits 
  my $low_lim = {};
  my $high_lim = {};
  my $cur_low = 0;
  my $cur_high = 0;
  foreach my $key (keys %{$weights}) {
    $low_lim->{$key} = $cur_low;
    $cur_high += $weights->{$key};
    $high_lim->{$key} = $cur_high;
    $cur_low += $weights->{$key};
  }
  $self->{'high_lim'} = $high_lim;
  $self->{'low_lim'} = $low_lim;
}

sub adjust_key {
  my($self, $key, $new_weight) = @_;

  my $weights = $self->{'weights'};
  $weights->{$key} = $new_weight;

  $self->adjust($weights);
} 

sub roll {
  my($self) = @_;
  return undef unless ref $self;

  my $rnd = rand($self->{'weight_total'});
  foreach my $key (keys %{$self->{'weights'}}) {
    my $high = $self->{'high_lim'}->{$key};
    my $low = $self->{'low_lim'}->{$key};
    return $key if(($rnd >= $low) && ($rnd <= $high));
  }
}


1;
__END__

