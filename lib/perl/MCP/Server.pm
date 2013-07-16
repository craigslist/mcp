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

package MCP::Server;
use base 'MCP::Object';
use Tie::Syslog;
use strict;

sub proto { 
  my($caller,$args) = @_;
  return {
    %{ shift->SUPER::proto(@_) },
    config       => $args->{'server_config'},
    log_file     => $args->{'log_file'},
    log_level    => $args->{'log_level'},
    syslog_ident => $args->{'syslog_ident'},
    log_handle   => undef,
  } 
}

sub init {
  my $self = shift;

  $self->_open_log_file();
  $self->_prepare_signal_handlers();
}

sub s_log {
  my($self,$level,$message) = @_;
  return undef unless ref $self;
  return undef unless($level <= $self->{'log_level'});
  my $lh = $self->{'log_handle'};

  # no logfile or syslog, spew to stderr
  unless($lh) {
    $message = scalar(localtime(time())).' '.$message."\n";
    print STDERR $message;
    return;
  }

  # if we're logging to a file, print date etc.:
  if($self->{'log_file'}) {
    $message = scalar(localtime(time())).' '.$message."\n";
  }

  print $lh $message;
  $lh->flush();
}


sub _open_log_file {
  my $self = shift;

  # order of priority: existing handle, syslog, log file, nothing.
  if(defined $self->{'log_handle'}) {
    # already have an active handle, just use it
    return undef;
  } elsif(my $syslog_id = $self->{'syslog_ident'}) {
    tie *LFH,    'Tie::Syslog', 'user.info', $syslog_id, 'pid', 'unix';
    tie *STDERR, 'Tie::Syslog', 'user.err',  $syslog_id, 'pid', 'unix';
    $self->{'log_handle'} = *LFH;
  } elsif(my $log_file = $self->{'log_file'}) {
    open(LFH, '>>'.$log_file) || die "failed to open $log_file for write: $!";
    open(STDERR, ">&LFH")     || die "failed to dup STDERR: $!";
    $self->{'log_handle'} = *LFH;
  } else {
    # we have neither a log file nor a syslog identity, so no setup required
    return undef;
  }
}

sub _close_log_file {
  my $self = shift;
  $self->{'log_handle'}->close();
}

sub change_user_group {
  my($self,$user,$group) = @_;

  my @uinfo = getpwnam($user);
  die "no such user $user" unless $uinfo[2];
  my @ginfo = getgrnam($group);
  die "no such group $group" unless $ginfo[2];

  $> = $uinfo[2]; # set effective user id
  $) = $ginfo[2]; # set effective group id

  return($uinfo[2],$ginfo[2]);
}

sub _prepare_signal_handlers { }

1;
__END__

