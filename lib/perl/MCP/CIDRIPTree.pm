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

package MCP::CIDRIPTree;

use strict;

use constant DEBUG => 0;

sub new {
  my($caller,$args) = @_;

  my $self = $args || {};
  bless $self, $caller;

  $self->{'tree'} = $args->{'tree'} || [];

  return $self;
}

sub clear {
  my($self) = @_;

  $self->{'tree'} = [];
}

sub get_tree {
  my($self) = @_;
  return undef unless ref $self;

  return $self->{'tree'};
}

# the tree is as follows
#
# depth from the top of the tree is the width of the cidr suffix; the
# top node is a.b.c.d/0
#
# each node is an anonymous array, where indexes are
#   0 => children of this address where this bit(depth) is zero
#   1 => children of this address where this bit(depth) is one
#   2 => a hash of prefixes stuffed at this cidr depth

sub add_ip {
  my($self,$args) = @_;
  my $cidrip = $args->{'ip'};
  my $key    = $args->{'key'};
  my $value  = $args->{'value'};
  return undef unless $self->_is_valid_cidrip($cidrip);

  my($ip,$mask) = split(/\//o, $cidrip);
  $mask = 32 unless defined $mask;
  my @ip_bits = $self->_ip_to_bits($ip);
  print STDERR "add_ip: ip " . $ip . " mask " . $mask . " bits " . join('', @ip_bits) . "\n" if DEBUG;

  # traverse the branches, adding branches as needed
  my $tref = $self->{'tree'};
  my $ltref = $tref;
  foreach my $i (0 .. ($mask-1)) {
    my $biznit = $ip_bits[$i];
    $ltref->[$biznit] ||= [];
    $ltref = $ltref->[$biznit];
  }

  # stick it in
  $ltref->[2]->{$key} = $value;

  # now don't forget
  $self->{'tree'} = $tref;
}

sub remove_ip {
  my($self,$args) = @_;
  my $cidrip = $args->{'ip'};
  my $key    = $args->{'key'};
  return undef unless $self->_is_valid_cidrip($cidrip);

  my($ip,$mask) = split(/\//o, $cidrip);
  $mask = 32 unless defined $mask;
  my @ip_bits = $self->_ip_to_bits($ip);
  print STDERR "remove_ip: ip " . $ip . " mask " . $mask . " bits " . join('', @ip_bits) . "\n" if DEBUG;

  # walk through the tree, paying attention to other branches for pruning
  my $tref = $self->{'tree'};
  my $ltref = $tref;
  my $last_branchybranch_bit = undef;
  my $last_branchybranch_ref = undef;
  my $last_branchybranch_lev = undef;
  foreach my $i (0 .. ($mask-1)) {
    my $biznit = $ip_bits[$i];
    $ltref->[$biznit] ||= [];
    if(ref $ltref->[$biznit ^ 1] || (ref $ltref->[2] && ($i < ($mask-1)))) {
      $last_branchybranch_ref = $ltref;
      $last_branchybranch_bit = $biznit;
      $last_branchybranch_lev = $i;
    }
    $ltref = $ltref->[$biznit];
  }
  my $there_be_other_keys = 1;
  if(defined $key) {
    delete $ltref->[2]->{$key} if(ref $ltref->[2] && $ltref->[2]->{$key});
    unless(scalar keys %{$ltref->[2]}) {
      delete $ltref->[2] if(ref $ltref->[2]);
      $there_be_other_keys = 0;
    }
  }
  else {
    delete $ltref->[2] if(ref $ltref->[2]);
    $there_be_other_keys = 0;
  }

  # prune the by_ip tree up to another listed block
  if($last_branchybranch_ref && ($last_branchybranch_lev != ($mask - 1))) {
    unless($there_be_other_keys) {
      delete $last_branchybranch_ref->[$last_branchybranch_bit];
    }
  }

  $self->{'tree'} = $tref;
}

# FIXME: need a 'fast' option to return the first match found rather
# than all matches

sub match_ip {
  my($self,$args) = @_;
  my $cidrip = $args->{'ip'};
  my $key    = $args->{'key'};
  return () unless $self->_is_valid_cidrip($cidrip);

  my @matches;

  my($ip,$mask) = split(/\//o, $cidrip);
  $mask = 32 unless defined $mask;
  my @ip_bits = $self->_ip_to_bits($ip);
  print STDERR "match_ip: ip " . $ip . " mask " . $mask . " bits " . join('', @ip_bits) . "\n" if DEBUG;

  # walk the tree, keep a list of any matching keys
  my $tref = $self->{'tree'};
  my $ltref = $tref;
  foreach my $i (0 .. $mask) {
    my $biznit = $ip_bits[$i];
	print STDERR "i $i biznit " . (defined $biznit ? $biznit : 'undef') . " ltref 0=" . (ref($ltref->[0])?'yes':'no') . " 1=" . (ref($ltref->[1])?'yes':'no') . " 2=" . (ref($ltref->[2])?'yes':'no') . "\n" if DEBUG;
	if(ref $ltref->[2]) {
      if(defined $key) {
        push(@matches, $ltref->[2]->{$key})
          if(exists $ltref->[2]->{$key});
      }
      else {
        push(@matches, $ltref->[2]);
      }
    }
	# choose the next level of the tree
    last unless (defined $biznit && ref($ltref->[$biznit]));
    $ltref = $ltref->[$biznit];
  }

  return @matches;
}



sub _ip_to_bits {
  my($caller, $ip) = @_;

  my @bits = ();

  my @quad = split(/\./, $ip);
  my $ip_val = 0;
  for my $o (0..3) {
	  $ip_val <<= 8;
	  $ip_val |= $quad[$o];
  }
  $ip_val = pack("V", $ip_val);
  foreach my $i (0 .. 31) {
    unshift @bits, vec($ip_val,$i,1);
  }

  return @bits;
}

sub _is_valid_cidrip {
  my($caller,$cidrip) = @_;

  return 1 if($cidrip =~ /^\d+\.\d+\.\d+\.\d+\/\d+$/o);
  return 1 if($cidrip =~ /^\d+\.\d+\.\d+\.\d+$/o);

  return undef;
}


# return a hash containing all of the data stashed inside of the tree
sub entries {
	my ($self) = @_;

	my $ret = {};
	my @tovisit = ();
	push(@tovisit, $self->{'tree'});
	while(scalar(@tovisit)) {
		my $this = shift(@tovisit);
		next unless ref($this) eq 'ARRAY';
		if (ref($this->[2]) eq 'HASH') {
			for my $k (keys(%{$this->[2]})) {
				$ret->{$k} = $this->[2]->{$k};
			}
		}
		push(@tovisit, @{$this->[0]}) if ref($this->[0]) eq 'ARRAY';
		push(@tovisit, @{$this->[1]}) if ref($this->[1]) eq 'ARRAY';
	}

	return $ret;
}


1;
__END__
