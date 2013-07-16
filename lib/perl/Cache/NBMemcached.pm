# $Id: NBMemcached.pm,v 1.6 2008/09/03 20:55:21 chaley Exp $
#
# Copyright (c) 2003, 2004  Brad Fitzpatrick <brad@danga.com>
#
# See COPYRIGHT section in pod text below for usage and distribution rights.
#

package Cache::NBMemcached;

use strict;
no strict 'refs';
use Storable ();
use Data::Storable ();
use Socket qw( MSG_NOSIGNAL PF_INET IPPROTO_TCP SOCK_STREAM );
use IO::Handle ();
use Time::HiRes ();
use String::CRC32;
use Errno qw( EINPROGRESS EWOULDBLOCK EISCONN );
use Epolld::Settings;

use fields qw{
    debug no_rehash stats compress_threshold compress_enable stat_callback
    readonly select_timeout namespace namespace_len servers active buckets
    pref_ip
    bucketcount _single_sock _stime
    nb 
    socks_for_filenos socks_for_crazy
    read_socks write_socks stuck_socks stuck_sock_timeout
    sockmode sockrq_read sockrq_write nbsock prev_sockmode
    oneline_state oneline_line oneline_ret
    delete_key delete_stime
    _set_key _set_line _set_stime _set_cmdname
    _incrdecr_key _incrdecr_line _incrdecr_stime _incrdecr_cmdname
    load_multi_reading load_multi_writing load_multi_state load_multi_buf 
    load_multi_offset load_multi_key load_multi_s_keys load_multi_flags
    load_multi_ret
    load_multi_sub_read load_multi_sub_write load_multi_sub_finalize
    load_multi_sub_dead
    connection_module
};

# A short note about the non-blocking mode of this module.  On top of
# the original Cache::Memcached, we've added an extra queue.  This
# means for every operation (set, get, incr, delete, etc.) we first
# check to see if there is an operation currently in progress.  If so,
# we add a callback for the same operation and push it onto the queue
# of delayed operations.  Then, when the current operation finishes,
# the queue is examined for delayed items.  If found, the first one in
# the queue is popped and executed.
#
# In order to prevent a thundering herd of cache purge operations from
# interrupting our normal flow of gets, we maintain two queues to
# separate memcache 'get' operations from everything else.  The read
# queue is always drained before the write queue is examined.

# flag definitions
use constant F_STORABLE => 1;
use constant F_COMPRESS => 2;

# size savings required before saving compressed value
use constant COMPRESS_SAVINGS => 0.20; # percent

use constant DEFAULT_STUCK_SOCK_TIMEOUT => 100; # seconds

use vars qw($VERSION $HAVE_ZLIB $FLAG_NOSIGNAL);
$VERSION = "1.14";

BEGIN {
    $HAVE_ZLIB = eval "use Compress::Zlib (); 1;";
}

$FLAG_NOSIGNAL = 0;
eval { $FLAG_NOSIGNAL = MSG_NOSIGNAL; };

my %host_dead;   # host -> unixtime marked dead until
my %cache_sock;  # host -> socket

my $PROTO_TCP;

our $SOCK_TIMEOUT = 2.6; # default timeout in seconds

sub new {
    my Cache::NBMemcached $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my ($args) = @_;

    $self->set_servers($args->{'servers'});
    $self->{'debug'} = Epolld::Settings::DEBUG || $args->{'debug'} || 0;
    $self->{'no_rehash'} = $args->{'no_rehash'};
    $self->{'stats'} = {};
    $self->{'pref_ip'} = $args->{'pref_ip'} || {};
    $self->{'compress_threshold'} = $args->{'compress_threshold'};
    $self->{'compress_enable'}    = 1;
    $self->{'stat_callback'} = $args->{'stat_callback'} || undef;
    $self->{'readonly'} = $args->{'readonly'};
    $self->{'nb'} = $args->{'nonblocking'}? 1: 0;
    $self->{'read_socks'} = {};
    $self->{'write_socks'} = {};
    $self->{'connection_module'} = $args->{'connection_module'};

    # TODO: undocumented
    $self->{'select_timeout'} = $args->{'select_timeout'} || 1.0;
    $self->{'stuck_sock_timeout'} = $args->{'stuck_sock_timeout'} ||
                                    DEFAULT_STUCK_SOCK_TIMEOUT;
    $self->{namespace} = $args->{namespace} || '';
    $self->{namespace_len} = int(length($self->{namespace}));

    return $self;
}

sub get_cache_socket_info {
  my($self) = shift;

  my $all_socks = { %cache_sock, %host_dead };

  my @sock_info;
  foreach my $h (sort(keys(%{$all_socks}))) {
    my $s = $cache_sock{$h};
    my $dead = $host_dead{$h}? ($cache_sock{$h}? 0: 1):0;
    push(@sock_info, [
      $h, ($dead? 'DEAD': 'active'),
      scalar(@{$self->{'sockrq_read'}->{$s}}) + scalar(@{$self->{'sockrq_write'}->{$s}}),
      ($self->{'read_socks'}->{$s}? 'R':'-').
      ($self->{'write_socks'}->{$s}? 'W':'-'),
      ($self->{'sockmode'}->{$s}? $self->{'sockmode'}->{$s}:'-'),
      ($self->{'prev_sockmode'}->{$s}? $self->{'prev_sockmode'}->{$s}:'-'),
    ]);
  }

  return @sock_info;
}

sub clear_stuck_socks {
  my($self,$now) = @_;
  print STDERR __PACKAGE__ . "::clear_stuck_socks\n" if Epolld::Settings::DEBUG;
  foreach my $ssock (keys %{$self->{'stuck_socks'}}) {
    next unless(($self->{'stuck_socks'}->{$ssock} + 
                 $self->{'stuck_sock_timeout'}) < $now);
    delete $self->{'read_socks'}->{$ssock};
    delete $self->{'write_socks'}->{$ssock};
    $self->{'oneline_state'}->{$ssock} = undef;
    $self->{'oneline_line'}->{$ssock} = undef;
    $self->{'oneline_ret'}->{$ssock} = undef;
    $self->{'connection_module'}->sock_watch(
      $self->{'socks_for_crazy'}->{$ssock},0,0)
      if($self->{'connection_module'});
    # FIXME: should the following 'delete' statement happen before we
    # call sock_watch?  Does it matter?
    delete $self->{'stuck_socks'}->{$ssock};
    $self->{'prev_sockmode'}->{$ssock} = $self->{'sockmode'}->{$ssock}
      if($self->{'sockmode'}->{$ssock});
    $self->{'sockmode'}->{$ssock} = undef;
    print STDERR __PACKAGE__ . "::clear_stuck_socks: $ssock\n";
    _dead_sock($ssock);
  }

  foreach my $rsock (keys %{$self->{'read_socks'}}) {
    $self->{'stuck_socks'}->{$rsock} = $now;
  }
  foreach my $wsock (keys %{$self->{'write_socks'}}) {
    $self->{'stuck_socks'}->{$wsock} = $now;
  }
}

sub set_pref_ip {
    my Cache::NBMemcached $self = shift;
    $self->{'pref_ip'} = shift;
}

sub set_servers {
    my Cache::NBMemcached $self = shift;
    my ($list) = @_;
    $self->{'servers'} = $list || [];
    $self->{'active'} = scalar @{$self->{'servers'}};
    $self->{'buckets'} = undef;
    $self->{'bucketcount'} = 0;

    $self->{'_single_sock'} = undef;
    if (@{$self->{'servers'}} == 1) {
        $self->{'_single_sock'} = $self->{'servers'}[0];
    }

    return $self;
}

sub set_debug {
    my Cache::NBMemcached $self = shift;
    my ($dbg) = @_;
    $self->{'debug'} = $dbg;
}

sub set_readonly {
    my Cache::NBMemcached $self = shift;
    my ($ro) = @_;
    $self->{'readonly'} = $ro;
}

sub set_norehash {
    my Cache::NBMemcached $self = shift;
    my ($val) = @_;
    $self->{'no_rehash'} = $val;
}

sub set_compress_threshold {
    my Cache::NBMemcached $self = shift;
    my ($thresh) = @_;
    $self->{'compress_threshold'} = $thresh;
}

sub enable_compress {
    my Cache::NBMemcached $self = shift;
    my ($enable) = @_;
    $self->{'compress_enable'} = $enable;
}

sub forget_dead_hosts {
    %host_dead = ();
}

sub set_stat_callback {
    my Cache::NBMemcached $self = shift;
    my ($stat_callback) = @_;
    $self->{'stat_callback'} = $stat_callback;
}

sub _dead_sock {
    my ($sock, $ret, $dead_for) = @_;
    print STDERR __PACKAGE__ . "::_dead_sock $sock\n" if Epolld::Settings::DEBUG;
    if ($sock =~ /^Sock_(.+?):(\d+)$/) {
        my $now = time();
        my ($ip, $port) = ($1, $2);
        my $host = "$ip:$port";
#        print STDERR __PACKAGE__ . "::dead_sock DEAD SOCK $sock\n" unless($host_dead{$host});
        $host_dead{$host} = $now + $dead_for
            if $dead_for;
        delete $cache_sock{$host};
    }
    return $ret;  # 0 or undef, probably, depending on what caller wants
}

sub _close_sock {
    my ($sock) = @_;
    print STDERR __PACKAGE__ . "::_close_sock $sock\n" if Epolld::Settings::DEBUG;
    if ($sock =~ /^Sock_(.+?):(\d+)$/) {
        my ($ip, $port) = ($1, $2);
        my $host = "$ip:$port";
        close $sock;
        delete $cache_sock{$host};
    }
}

sub _connect_sock { # sock, sin, timeout
    my ($sock, $sin, $timeout) = @_;
    $timeout ||= 0.25;

    print STDERR __PACKAGE__ . "::_connect_sock $sock $timeout\n" if Epolld::Settings::DEBUG;

    # make the socket non-blocking from now on,
    # except if someone wants 0 timeout, meaning
    # a blocking connect, but even then turn it
    # non-blocking at the end of this function

    if ($timeout) {
        IO::Handle::blocking($sock, 0);
    } else {
        IO::Handle::blocking($sock, 1);
    }

    my $ret = connect($sock, $sin);

    if (!$ret && $timeout && $!==EINPROGRESS) {

        my $win='';
        vec($win, fileno($sock), 1) = 1;

        if (select(undef, $win, undef, $timeout) > 0) {
            $ret = connect($sock, $sin);
            # EISCONN means connected & won't re-connect, so success
            $ret = 1 if !$ret && $!==EISCONN;
        }
    }

    unless ($timeout) { # socket was temporarily blocking, now revert
        IO::Handle::blocking($sock, 0);
    }

    # from here on, we use non-blocking (async) IO for the duration
    # of the socket's life

    print STDERR __PACKAGE__ . "::_connect_sock $sock $timeout ret " . ($ret // 'undef') . "\n" if Epolld::Settings::DEBUG;

    return $ret;
}

sub sock_to_host { # (host)
    my Cache::NBMemcached $self = ref $_[0] ? shift : undef;
    my $host = $_[0];

    print STDERR __PACKAGE__ . "::sock_to_host $host\n" if Epolld::Settings::DEBUG;
    if ($cache_sock{$host}) {
	    print STDERR __PACKAGE__ . "::sock_to_host $host cached\n" if Epolld::Settings::DEBUG;
	    return $cache_sock{$host};
    }

    my $now = time();
    my ($ip, $port) = $host =~ /(.*):(\d+)/;
    if ($host_dead{$host} && $host_dead{$host} > $now) {
	    print STDERR __PACKAGE__ . "::sock_to_host $host dead\n" if Epolld::Settings::DEBUG;
	    return undef;
    }
    my $sock = "Sock_$host";

    my $connected = 0;
    my $sin;
    my $proto = $PROTO_TCP ||= getprotobyname('tcp');

    # if a preferred IP is known, try that first.
    if ($self && $self->{pref_ip}{$ip}) {
        socket($sock, PF_INET, SOCK_STREAM, $proto);
        my $prefip = $self->{pref_ip}{$ip};
        $sin = Socket::sockaddr_in($port,Socket::inet_aton($prefip));
        if (_connect_sock($sock,$sin,0.1)) {
            $connected = 1;
        } else {
            close $sock;
        }
    }

    # normal path, or fallback path if preferred IP failed
    unless ($connected) {
        socket($sock, PF_INET, SOCK_STREAM, $proto);
        $sin = Socket::sockaddr_in($port,Socket::inet_aton($ip));
        unless (_connect_sock($sock,$sin)) {
	        print STDERR __PACKAGE__ . "::sock_to_host $host connect fail\n" if Epolld::Settings::DEBUG;
            return _dead_sock($sock, undef, 20 + int(rand(10)));
        }
    }

    # make the new socket not buffer writes.
    my $old = select($sock);
    $| = 1;
    select($old);

    # remember some other stuff about this socket
    my $sfileno = fileno($sock);
    $self->{'socks_for_filenos'}->{$sfileno} = $sock;
    my $s = $self->{'socks_for_crazy'}->{$sock};
    open($s, "+<&=$sock");
    $self->{'socks_for_crazy'}->{$sock} = $s;
    delete $self->{'read_socks'}->{$sock};
    delete $self->{'write_socks'}->{$sock};
    $self->{'connection_module'}->sock_new($s)
      if($self->{'connection_module'});
    $self->{'prev_sockmode'}->{$sock} = $self->{'sockmode'}->{$sock}
      if($self->{'sockmode'}->{$sock});
    $self->{'sockmode'}->{$sock} = undef;
    $self->{'nbsock'} = undef;
    delete $host_dead{$host};
    $cache_sock{$host} = $sock;
    #print STDERR "NEW SOCK: $sock, $s, $sfileno\n";

    # warn about overflowing sock request queues
    for my $q ('sockrq_read', 'sockrq_write') {
	    if($self->{$q}->{$sock} && (scalar(@{$self->{$q}->{$sock}}) > 10)) {
		    print STDERR __PACKAGE__ . "::sock_to_host $q length = ".scalar(@{$self->{$q}->{$sock}})."\n";
	    }
    }

    $self->_start_next_request($sock);

    print STDERR __PACKAGE__ . "::sock_to_host $host $sock\n" if Epolld::Settings::DEBUG;

    return $sock;
}

sub get_sock { # (key)
    my Cache::NBMemcached $self = shift;
    my ($key) = @_;
    print STDERR __PACKAGE__ . "::get_sock key $key\n" if Epolld::Settings::DEBUG;
    if ($self->{'_single_sock'}) {
	    print STDERR __PACKAGE__ . "::get_sock key $key single\n" if Epolld::Settings::DEBUG;
	    return $self->sock_to_host($self->{'_single_sock'})
    }
    unless ($self->{'active'}) {
	    print STDERR __PACKAGE__ . "::get_sock key $key inactive\n" if Epolld::Settings::DEBUG;
	    return undef;
    }
    my $hv = ref $key ? int($key->[0]) : _hashfunc($key);

    $self->init_buckets() unless $self->{'buckets'};

    my $real_key = ref $key ? $key->[1] : $key;
    my $tries = 0;
    while ($tries++ < 20) {
	    print STDERR __PACKAGE__ . "::get_sock key $key try $tries\n" if Epolld::Settings::DEBUG;
        my $host = $self->{'buckets'}->[$hv % $self->{'bucketcount'}];
        my $sock = $self->sock_to_host($host);
	    if ($sock) {
		    print STDERR __PACKAGE__ . "::get_sock key $key sock $sock\n" if Epolld::Settings::DEBUG;
		    return $sock;
	    }
	    if ($sock->{'no_rehash'}) {
		    print STDERR __PACKAGE__ . "::get_sock key $key no rehash\n" if Epolld::Settings::DEBUG;
		    return undef;
	    }
        $hv += _hashfunc($tries . $real_key);  # stupid, but works
    }
    print STDERR __PACKAGE__ . "::get_sock key $key max retries\n" if Epolld::Settings::DEBUG;
    return undef;
}

sub init_buckets {
    my Cache::NBMemcached $self = shift;
    return if $self->{'buckets'};
    my $bu = $self->{'buckets'} = [];
    foreach my $v (@{$self->{'servers'}}) {
        if (ref $v eq "ARRAY") {
            for (1..$v->[1]) { push @$bu, $v->[0]; }
        } else {
            push @$bu, $v;
        }
    }
    $self->{'bucketcount'} = scalar @{$self->{'buckets'}};
}

sub disconnect_all {
    my $sock;
    foreach $sock (values %cache_sock) {
        close $sock;
    }
    %cache_sock = ();
}

sub _oneline {
    use bytes;
    my Cache::NBMemcached $self = shift;
    my ($sock, $line, $nbsock) = @_;
    my $res;
    my ($ret, $offset) = (undef, 0);

    # state: 0 - writing, 1 - reading, 2 - done
    my $state = defined $line ? 0 : 1;

    if (Epolld::Settings::DEBUG) {
	my $l = (ref $line ? $$line : $line);
	$l //= 'undef';
	$l =~ s/\r/\\r/g;
	$l =~ s/\n/\\n/g;

	print STDERR __PACKAGE__ . "::_oneline sock=" . ($sock // 'undef')
	    . " nbsock=" . ($nbsock // 'undef')
	    . " state=" . ($state ? 'reading' : 'writing')
	    . " line=" . $l
	    . "\n";
    }

    # if no nonblock socket was passed, assume this is our first time in
    if($self->{'nb'} && !$nbsock) {

      # based on read or write state, update socket watch hashes
      if($state == 0) {
	print STDERR __PACKAGE__ . "::_oneline sock=" . ($sock // 'undef')
	    . " watch write\n" if Epolld::Settings::DEBUG;
        delete $self->{'read_socks'}->{$sock};
        $self->{'write_socks'}->{$sock} = $sock;
        $self->{'connection_module'}->sock_watch(
          $self->{'socks_for_crazy'}->{$sock},0,1)
          if($self->{'connection_module'});
      }
      elsif($state == 1) {
	print STDERR __PACKAGE__ . "::_oneline sock=" . ($sock // 'undef')
	    . " watch read\n" if Epolld::Settings::DEBUG;
        delete $self->{'write_socks'}->{$sock};
        $self->{'read_socks'}->{$sock} = $sock;
        $self->{'connection_module'}->sock_watch(
          $self->{'socks_for_crazy'}->{$sock},1,0)
          if($self->{'connection_module'});
      }

      # remember important values
      $self->{'oneline_state'}->{$sock} = $state;
      $self->{'oneline_line'}->{$sock}  = $line;
      $self->{'oneline_ret'}->{$sock}   = $ret;

      print STDERR __PACKAGE__ . "::_oneline first time setup, return undef\n" if Epolld::Settings::DEBUG;
      return undef;
    }
 
    # if a nonblock socket was passed, assume we're in the middle of something
    if($nbsock) {
      $state = $self->{'oneline_state'}->{$sock};

      # writing state
      if($state == 0) {
        $res = send($sock, ${$self->{'oneline_line'}->{$sock}}, $FLAG_NOSIGNAL);
	print STDERR __PACKAGE__ . "::_oneline nb write res=" . ($res // 'undef') . "\n" if Epolld::Settings::DEBUG;
	if (not defined $res and $!==EWOULDBLOCK) {
	    print STDERR __PACKAGE__ . "::_oneline WOULDBLOCK, return undef\n";
	    return undef 
	}
        unless ($res > 0) {
	    print STDERR __PACKAGE__ . "::_oneline closing after write, return undef\n" if Epolld::Settings::DEBUG;
            $self->{'oneline_state'}->{$sock} = 0;
            $self->{'oneline_line'}->{$sock} = undef;
            delete $self->{'read_socks'}->{$sock};
            delete $self->{'write_socks'}->{$sock};
            $self->{'connection_module'}->sock_delete(
              $self->{'socks_for_crazy'}->{$sock})
              if($self->{'connection_module'});
            _close_sock($sock);
            return undef;
        }
        if ($res == int(length(${$self->{'oneline_line'}->{$sock}}))) { # all sent
	    print STDERR __PACKAGE__ . "::_oneline nb write complete, watch read\n" if Epolld::Settings::DEBUG;
            $state = $self->{'oneline_state'}->{$sock} = 1;
            $self->{'oneline_line'}->{$sock} = undef;
            delete $self->{'write_socks'}->{$sock};
            $self->{'read_socks'}->{$sock} = $sock;
            $self->{'connection_module'}->sock_watch(
              $self->{'socks_for_crazy'}->{$sock},1,0)
              if($self->{'connection_module'});
        } else { # we only succeeded in sending some of it
	    print STDERR __PACKAGE__ . "::_oneline nb write partial\n" if Epolld::Settings::DEBUG;
            substr(${$self->{'oneline_line'}->{$sock}}, 0, $res, ''); 
        }
      }

      # reading state
      elsif($state == 1) {
        $res = sysread($sock, $self->{'oneline_ret'}->{$sock}, 255, $offset);
	print STDERR __PACKAGE__ . "::_oneline nb read res=" . ($res // 'undef') . "\n" if Epolld::Settings::DEBUG;
	if (!defined($res) and $!==EWOULDBLOCK) {
	    print STDERR __PACKAGE__ . "::_oneline WOULDBLOCK, return undef\n";
	    return undef;
	}
        if ($res == 0) { # catches 0=conn closed or undef=error
	    print STDERR __PACKAGE__ . "::_oneline nb read close on res=0, return undef\n" if Epolld::Settings::DEBUG;
            $self->{'oneline_state'}->{$sock} = 0;
            delete $self->{'read_socks'}->{$sock};
            delete $self->{'write_socks'}->{$sock};
            $self->{'connection_module'}->sock_delete(
              $self->{'socks_for_crazy'}->{$sock})
              if($self->{'connection_module'});
            _close_sock($sock);
            return undef;
        }
        $offset += $res;
        if(rindex($self->{'oneline_ret'}->{$sock},"\r\n") + 2 == 
           int(length($self->{'oneline_ret'}->{$sock}))) {
	    print STDERR __PACKAGE__ . "::_oneline nb read complete, unwatch\n" if Epolld::Settings::DEBUG;
            $state = $self->{'oneline_state'}->{$sock} = 2;
            delete $self->{'read_socks'}->{$sock};
            delete $self->{'write_socks'}->{$sock};
            $self->{'connection_module'}->sock_watch(
              $self->{'socks_for_crazy'}->{$sock},0,0)
              if($self->{'connection_module'});
        }
      }
 
      # finished state, return response
      if($state == 2) {
	  if (Epolld::Settings::DEBUG) {
	      my $r = ($self->{'oneline_ret'}->{$sock} // 'undef');
	      $r =~ s/\r/\\r/g;
	      $r =~ s/\n/\\n/g;
	      print STDERR __PACKAGE__ . "::_oneline nb finish, return " . $r . "\n";
	  }
        $self->{'oneline_line'}->{$sock} = undef; 
        return $self->{'oneline_ret'}->{$sock};
      }

      print STDERR __PACKAGE__ . "::_oneline fallthrough, return undef\n" if Epolld::Settings::DEBUG;
      return undef;
    }

    # the bitsets for select
    my ($rin, $rout, $win, $wout);
    my $nfound;

    my $copy_state = -1;
    local $SIG{'PIPE'} = "IGNORE" unless $FLAG_NOSIGNAL;

    # the select loop
    while(1) {
        if ($copy_state!=$state) {
            last if $state==2;
            ($rin, $win) = ('', '');
            vec($rin, fileno($sock), 1) = 1 if $state==1;
            vec($win, fileno($sock), 1) = 1 if $state==0;
            $copy_state = $state;
        }
        $nfound = select($rout=$rin, $wout=$win, undef,
                         $self->{'select_timeout'});
        last unless $nfound;

        if (vec($wout, fileno($sock), 1)) {
            $res = send($sock, $line, $FLAG_NOSIGNAL);
	    print STDERR __PACKAGE__ . "::_oneline b write res=" . ($res // 'undef') . "\n" if Epolld::Settings::DEBUG;
            next
                if not defined $res and $!==EWOULDBLOCK;
            unless ($res > 0) {
		print STDERR __PACKAGE__ . "::_oneline b write failed\n" if Epolld::Settings::DEBUG;
                $self->{'oneline_state'}->{$sock} = 0;
                _close_sock($sock);
                return undef;
            }
            if ($res == int(length($line))) { # all sent
		print STDERR __PACKAGE__ . "::_oneline b write complete\n" if Epolld::Settings::DEBUG;
                $state = 1;
            } else { # we only succeeded in sending some of it
		print STDERR __PACKAGE__ . "::_oneline b write partial\n" if Epolld::Settings::DEBUG;
                substr($line, 0, $res, ''); # delete the part we sent
            }
        }

        if (vec($rout, fileno($sock), 1)) {
            $res = sysread($sock, $ret, 255, $offset);
	    print STDERR __PACKAGE__ . "::_oneline b read res=" . ($res // 'undef') . "\n" if Epolld::Settings::DEBUG;
            next
                if !defined($res) and $!==EWOULDBLOCK;
            if ($res == 0) { # catches 0=conn closed or undef=error
		print STDERR __PACKAGE__ . "::_oneline b read close\n" if Epolld::Settings::DEBUG;
                $self->{'oneline_state'}->{$sock} = 0;
                _close_sock($sock);
                return undef;
            }
            $offset += $res;
            if (rindex($ret, "\r\n") + 2 == int(length($ret))) {
                $state = 2;
            }
        }
    }

    unless ($state == 2) {
	print STDERR __PACKAGE__ . "::_oneline b read state=2\n" if Epolld::Settings::DEBUG;
        $self->{'oneline_state'}->{$sock} = 0;
        delete $self->{'read_socks'}->{$sock};
        delete $self->{'write_socks'}->{$sock};
        delete $self->{'stuck_socks'}->{$sock};
        $self->{'connection_module'}->sock_delete(
          $self->{'socks_for_crazy'}->{$sock})
          if($self->{'connection_module'});
        $self->{'prev_sockmode'}->{$sock} = $self->{'sockmode'}->{$sock}
          if($self->{'sockmode'}->{$sock});
        $self->{'sockmode'}->{$sock} = undef;
        _dead_sock($sock); # improperly finished
        return undef;
    }

    return $ret;
}


sub delete {
    my Cache::NBMemcached $self = shift;
    my ($key, $time) = @_;

    my $nbsock = $self->{'nbsock'};
    $self->{'nbsock'} = undef;

    return 0 if ! $self->{'active'} || $self->{'readonly'};
    my $stime = Time::HiRes::time() if $self->{'stat_callback'};
    my $sock = $nbsock? $nbsock: $self->get_sock($key);
    return 0 unless $sock;

    # check sockmode, put request in queue and return if sock busy
    if($self->{'nb'} && $self->{'sockmode'}->{$sock}) {
      unless(($self->{'sockmode'}->{$sock} eq 'delete') && $nbsock) {
	  print STDERR __PACKAGE__ . "::delete $sock sockmode " . $self->{'sockmode'}->{$sock} . " cmdname delete\n" if Epolld::Settings::DEBUG;
        $self->{'sockrq_write'}->{$sock} ||= [];
        push(@{$self->{'sockrq_write'}->{$sock}}, sub {
          $self->delete($key,$time);
        });
        return;
      }
    }
    $self->{'sockmode'}->{$sock} = 'delete';
    print STDERR __PACKAGE__ . "::delete $sock set sockmode delete\n" if Epolld::Settings::DEBUG;

    my $res;
    # nonblocking subsequent iterations
    if($nbsock) {
      $key   = $self->{'delete_key'}->{$sock};
      $stime = $self->{'delete_stime'}->{$sock};
      $res = _oneline($self, $sock, undef, $nbsock); 
    }

    # blocking or nonblocking first iteration
    else {
      $self->{'delete_key'}->{$sock}   = $key;
      $self->{'delete_stime'}->{$sock} = $stime;
      $self->{'stats'}->{"delete"}++;
      $key = ref $key ? $key->[1] : $key;
      $time = $time ? " $time" : "";
      my $cmd = "delete $self->{namespace}$key$time\r\n";
      $res = _oneline($self, $sock, \$cmd);
    }

    # if we're not nonblocking or we are but have a result, finalize things
    if(!$self->{'nb'} || $res) {
      if ($self->{'stat_callback'}) {
        my $etime = Time::HiRes::time();
        $self->{'stat_callback'}->($stime, $etime, $sock, 'delete');
      }
      delete $self->{'delete_stime'}->{$sock};
    }

    if($self->{'nb'}) {
      my @response;
      if($res) {
        $self->{'prev_sockmode'}->{$sock} = $self->{'sockmode'}->{$sock}
          if($self->{'sockmode'}->{$sock});
        $self->{'sockmode'}->{$sock} = undef;
        @response = ( { type => $res, key => $key, cmd => 'DELETE' } );
      }
      return @response;
    } 
    else {
      return $res eq "DELETED\r\n";
    }
}

sub add {
    _set("add", @_);
}

sub replace {
    _set("replace", @_);
}

sub set {
    _set("set", @_);
}

sub _set {
    my $cmdname = shift;
    my Cache::NBMemcached $self = shift;
    my ($key, $val, $exptime) = @_;

    my $nbsock = $self->{'nbsock'};
    $self->{'nbsock'} = undef;

    return 0 if ! $self->{'active'} || $self->{'readonly'};
    my $sock = $nbsock? $nbsock: $self->get_sock($key);
    return 0 unless $sock;

    # check sockmode, put request in queue and return if sock busy
    if($self->{'nb'} && $self->{'sockmode'}->{$sock}) {
      unless(($self->{'sockmode'}->{$sock} eq $cmdname) && $nbsock) {
	  print STDERR __PACKAGE__ . "::_set $sock sockmode " . $self->{'sockmode'}->{$sock} . " cmdname $cmdname\n" if Epolld::Settings::DEBUG;
        $self->{'sockrq_write'}->{$sock} ||= [];
        push(@{$self->{'sockrq_write'}->{$sock}}, sub {
          #$self->_set($cmdname,$key,$val,$exptime);
          _set($cmdname,$self,$key,$val,$exptime);
        });
        return;
      }
    }
    $self->{'sockmode'}->{$sock} = $cmdname;
    print STDERR __PACKAGE__ . "::_set $sock set sockmode $cmdname\n" if Epolld::Settings::DEBUG;

    my $line;
    my $res;
    my $stime;

    # nonblocking, subsequent iterations
    if($nbsock) {
      $key     = $self->{'_set_key'}->{$sock};
      $stime   = $self->{'_set_stime'}->{$sock};
      $cmdname = $self->{'_set_cmdname'}->{$sock};
      $res = _oneline($self, $sock, undef, $nbsock);
    }

    # nonblocking first iteration or blocking
    else {
      $self->{'_set_key'}->{$sock} = $key;
      $self->{'_set_cmdname'}->{$sock} = $cmdname;
      $stime = Time::HiRes::time() if $self->{'stat_callback'};
      $self->{'_set_stime'}->{$sock} = $stime;

      use bytes; # return bytes from length()

      $self->{'stats'}->{$cmdname}++;
      my $flags = 0;
      $key = ref $key ? $key->[1] : $key;

      if (ref $val) {
          $val = Data::Storable::nfreeze($val);
          $flags |= F_STORABLE;
      }

      # very important -- perl 5.14 returns undef for length(undef),
      # so we need to cast this to an int to get the right answer
      my $len = int(length($val));

      if ($self->{'compress_threshold'} && $HAVE_ZLIB && $self->{'compress_enable'} &&
          $len >= $self->{'compress_threshold'}) {

          my $c_val = Compress::Zlib::memGzip($val);
          my $c_len = int(length($c_val));

          # do we want to keep it?
          if ($c_len < $len*(1 - COMPRESS_SAVINGS)) {
              $val = $c_val;
              $len = $c_len;
              $flags |= F_COMPRESS;
	      print STDERR __PACKAGE__ . "::_set $sock set sockmode $cmdname -- using compressed version\n" if Epolld::Settings::DEBUG;
          }
      }

      $exptime = int($exptime || 0);

      local $SIG{'PIPE'} = "IGNORE" unless $FLAG_NOSIGNAL;
      $line = 
        "$cmdname $self->{namespace}$key $flags $exptime $len\r\n$val\r\n";

      $res = _oneline($self, $sock, \$line);
    }

    # if we're not nonblocking or we are but have a result, finalize things
    if(!$self->{'nb'} || $res) {

      if ($self->{'debug'} || Epolld::Settings::DEBUG) {
        print STDERR __PACKAGE__ . "::_set $cmdname $self->{namespace}$key = " . ($val // 'undef') . "\n";
      }

      if ($self->{'stat_callback'}) {
        my $etime = Time::HiRes::time();
        $self->{'stat_callback'}->($stime, $etime, $sock, $cmdname);
      }

      delete $self->{'_set_key'}->{$sock};
      delete $self->{'_set_stime'}->{$sock};
      delete $self->{'_set_cmdname'}->{$sock};
    }

    # return bool for blocking, response array for nonblocking
    if($self->{'nb'}) {
      my @response;
      if($res) {
        $self->{'prev_sockmode'}->{$sock} = $self->{'sockmode'}->{$sock}
          if($self->{'sockmode'}->{$sock});
        $self->{'sockmode'}->{$sock} = undef;
        $self->{'_set_line'}->{$sock} = undef;
        @response = ( { type => $res, key => $key, cmd => 'SET' } );
      }
      return @response;
    } 
    else {
      return $res eq "STORED\r\n";
    }
}

sub incr {
    _incrdecr("incr", @_);
}

sub decr {
    _incrdecr("decr", @_);
}

sub _incrdecr {
    my $cmdname = shift;
    my Cache::NBMemcached $self = shift;
    my ($key, $value) = @_;
    return undef if ! $self->{'active'} || $self->{'readonly'};

    my $nbsock = $self->{'nbsock'};
    $self->{'nbsock'} = undef;

    my $sock = $nbsock? $nbsock: $self->get_sock($key);
    return undef unless $sock;

    my @responses;

    # check sockmode, put request in queue and return if sock busy
    if($self->{'nb'} && $self->{'sockmode'}->{$sock}) {
      unless(($self->{'sockmode'}->{$sock} eq $cmdname) && $nbsock) {
	  print STDERR __PACKAGE__ . "::_incrdecr $sock sockmode " . $self->{'sockmode'}->{$sock} . " cmdname $cmdname\n" if Epolld::Settings::DEBUG;
        $self->{'sockrq_write'}->{$sock} ||= [];
        push(@{$self->{'sockrq_write'}->{$sock}}, sub {
          #$self->_incrdecr($cmdname,$key,$value);
          _incrdecr($cmdname,$self,$key,$value);
        });
        return;
      }
    }
    $self->{'sockmode'}->{$sock} = $cmdname;
    print STDERR __PACKAGE__ . "::_incrdecr $sock set sockmode $cmdname\n" if Epolld::Settings::DEBUG;

    my $res;
    my $line;
    my $stime;

    # nonblocking subsequent iterations
    if($nbsock) {
      $key     = $self->{'_incrdecr_key'}->{$sock};
      $line    = $self->{'_incrdecr_line'}->{$sock};
      $stime   = $self->{'_incrdecr_stime'}->{$sock};
      $cmdname = $self->{'_incrdecr_cmdname'}->{$sock};
      $res = _oneline($self, $nbsock, undef, $nbsock);      
    }
    
    # blocking or nonblocking first iteration
    else {
      $key = $key->[1] if ref $key;
      $self->{'stats'}->{$cmdname}++;
      $value = 1 unless defined $value;
      $self->{'_incrdecr_key'}->{$sock} = $key;
      $self->{'_incrdecr_cmdname'}->{$sock} = $cmdname;
      $stime = Time::HiRes::time() if $self->{'stat_callback'};
      $self->{'_incrdecr_stime'}->{$sock} = $stime;

      $line = "$cmdname $self->{namespace}$key $value\r\n";
      $res = _oneline($self, $sock, \$line);
    }

    if ($self->{'stat_callback'}) {
        my $etime = Time::HiRes::time();
        $self->{'stat_callback'}->($stime, $etime, $sock, $cmdname);
    }

    if($self->{'nb'}) {
      if($res) {
        $self->{'prev_sockmode'}->{$sock} = $self->{'sockmode'}->{$sock}
          if($self->{'sockmode'}->{$sock});
        $self->{'sockmode'}->{$sock} = undef;
        push(@responses, { type => $res, key => $key });
      }
      return @responses;
    }
    else {
      return undef unless $res =~ /^(\d+)/;
      return $1;
    }
}

sub get_watch_socks {
  my $self = shift;

   return {
     watch_read => { 
       map { $_ => $_ }  
           map { $self->{'socks_for_crazy'}->{$_} } 
               values %{$self->{'read_socks'}}
     },
     watch_write => { 
       map { $_ => $_ }  
           map { $self->{'socks_for_crazy'}->{$_} } 
               values %{$self->{'write_socks'}}
     },
   };
}

sub sock_ready {
  my($self,$sock) = @_;
  my $fileno = fileno($sock);
  $sock = $self->{'socks_for_filenos'}->{$fileno};
  delete $self->{'stuck_socks'}->{$sock};

  my @responses;
  if(my $sockmode = $self->{'sockmode'}->{$sock}) {
    $self->{'nbsock'} = $sock;
    push @responses, $self->$sockmode();
    $self->{'nbsock'} = undef;
  } 
  else {
    #TODO what to do here?
    print STDERR __PACKAGE__ . "::sock_ready called on sockmodeless sock\n";
  }

  # if response(s) returned, instigate the next request for this socket
#  if(scalar @responses && ($self->{'sockmode'}->{$sock} ne 'get_multi')) {
  unless($self->{'sockmode'}->{$sock}) {
    $self->_start_next_request($sock);
  }

  return \@responses;
}

# if there's an error on the wire and an external caller needed to
# close this socket, pass in the sock here and this will clean up
sub close_and_forget_sock {
  my($self,$perlsock) = @_;
  my $fileno = fileno($perlsock);
  my $sock = $self->{'socks_for_filenos'}->{$fileno};

  print STDERR __PACKAGE__ . "::close_and_forget_sock"
      . " perlsock=" . ($perlsock // 'undef')
      . " fileno=" . $fileno
      . " sock=" . ($sock // 'undef')
      . "\n" if Epolld::Settings::DEBUG;

  delete $self->{'read_socks'}->{$sock};
  delete $self->{'write_socks'}->{$sock};
  delete $self->{'stuck_socks'}->{$sock};
  $self->{'connection_module'}->sock_delete(
      $self->{'socks_for_crazy'}->{$sock})
      if($self->{'connection_module'});
  $self->{'prev_sockmode'}->{$sock} = $self->{'sockmode'}->{$sock}
      if($self->{'sockmode'}->{$sock});
  $self->{'sockmode'}->{$sock} = undef;
  _close_sock($sock);
}

sub _start_next_request {
  my($self,$sock) = @_;
  $self->{'prev_sockmode'}->{$sock} = $self->{'sockmode'}->{$sock}
    if($self->{'sockmode'}->{$sock});
  $self->{'sockmode'}->{$sock} = undef;

  # see if there are any queued requests we can work on, prioritizing
  # reads before writes
  if($self->{'sockrq_read'}->{$sock} && scalar @{$self->{'sockrq_read'}->{$sock}}) {
    my $request = shift @{$self->{'sockrq_read'}->{$sock}};
    $request->();
  } elsif($self->{'sockrq_write'}->{$sock} && scalar @{$self->{'sockrq_write'}->{$sock}}) {
    my $request = shift @{$self->{'sockrq_write'}->{$sock}};
    $request->();
  }
}

sub get {
    my Cache::NBMemcached $self = shift;
    my ($key) = @_;

    # TODO: make a fast path for this?  or just keep using get_multi?
    my $r = $self->get_multi($key);
    my $kval = ref $key ? $key->[1] : $key;
    return $r->{$kval};
}

sub get_multi {
    my Cache::NBMemcached $self = shift;
    my $nbsock = $self->{'nbsock'};
    $self->{'nbsock'} = undef;

    print STDERR __PACKAGE__ . "::get_multi nbsock=" . ($nbsock // 'undef') . "\n" if Epolld::Settings::DEBUG;

    unless ($self->{'active'}) {
	    print STDERR __PACKAGE__ . "::get_multi inactive\n" if Epolld::Settings::DEBUG;
	    # FIXME: what does this really mean?
	    return undef;
    }
    my %val;        # what we'll be returning a reference to (realkey -> value)
    my %sock_keys;  # sockref_as_scalar -> [ realkeys ]
    my $sock;
    my @responses;

    # nonblocking mode subsequent iterations
    if($nbsock) {
	    print STDERR __PACKAGE__ . "::get_multi nbsock load responses\n" if Epolld::Settings::DEBUG;
	    my @r = _load_multi($self, \%sock_keys, undef, $nbsock);
	    push(@responses, @r) if(scalar @r && defined $r[0]);
    }

    # blocking mode or nonblocking mode first iteration
    else {

      my %queue_sock_keys;
      foreach my $key (@_) {
        $sock = $self->get_sock($key);
        unless ($sock) {
	        print STDERR __PACKAGE__ . "::get_multi key $key sock failboat\n" if Epolld::Settings::DEBUG;
	        # failing to contact a cache server results is a MISS so that
	        # the calling code will go on to contact the origin servers
	        push(@responses, { 'type' => 'MISS', 'key' => $key });
	        next;
        }

        if($self->{'nb'} && $self->{'sockmode'}->{$sock}) {
	        # in nonblocking mode, remember keys for busy sockets
	        print STDERR __PACKAGE__ . "::get_multi key $key sock $sock adding to queue_sock_keys because sockmode " . $self->{'sockmode'}->{$sock} . "\n" if Epolld::Settings::DEBUG;
	        push(@{$queue_sock_keys{$sock}}, $key);
	        # ok to skip the rest of the processing here, because the
	        # presence of a value for sockmode means that the socket
	        # is already open and in use and will be tickled by epolld
	        # appropriately later
	        next;
        }

        $self->{'sockmode'}->{$sock} = 'get_multi';
	print STDERR __PACKAGE__ . "::get_multi $sock set sockmode get_multi\n" if Epolld::Settings::DEBUG;
        my $kval = ref $key ? $key->[1] : $key;
        push @{$sock_keys{$sock}}, $kval;
      }

      # queue new get_multi requests for busy sockets
      foreach my $qsock (keys %queue_sock_keys) {
        print STDERR __PACKAGE__ . "::get_multi requeue " . join(' ', @{$queue_sock_keys{$qsock}}) . "\n" if Epolld::Settings::DEBUG;
        my $now = Time::HiRes::gettimeofday();
        $self->{'sockrq_read'}->{$qsock} ||= [];
        push(@{$self->{'sockrq_read'}->{$qsock}}, sub {
          $self->get_multi(@{$queue_sock_keys{$qsock}});
        });
      }

      # initial stats
      $self->{'stats'}->{"get_keys"} += @_;
      $self->{'stats'}->{"get_socks"} += keys %sock_keys;
      $self->{'stats'}->{"get_multi"}++;
      $self->{'_stime'} = Time::HiRes::time() if $self->{'stat_callback'};

      local $SIG{'PIPE'} = "IGNORE" unless $FLAG_NOSIGNAL;

      _load_multi($self, \%sock_keys, \%val);
    }

    if($self->{'nb'}) {
	    # FIXME: when is 'nb' set when 'nbsock' is not?  This is
	    # confusing.  Seems like this logic can be combined with the
	    # if($nbsock) block above...
	    print STDERR __PACKAGE__ . "::get_multi return (num responses " . scalar(@responses) . ")\n" if Epolld::Settings::DEBUG;
	    #use Data::Dumper; print STDERR Dumper(\@responses) if Epolld::Settings::DEBUG;
      return @responses;
    }
    else {
      if ($self->{'debug'} || Epolld::Settings::DEBUG) {
          while (my ($k, $v) = each %val) {
              print STDERR "MemCache: got $k = $v\n";
          }
      }
      return \%val;
    }
}

sub _load_multi {
    use bytes; # return bytes from length()
    my Cache::NBMemcached $self = shift;
    my ($sock_keys, $ret, $nb_sock) = @_;

    if (Epolld::Settings::DEBUG) {
	my @sk = keys(%$sock_keys);
	my @rk = keys(%$ret);
	print STDERR __PACKAGE__ . "::_load_multi"
	    . " sock_keys " . (scalar(@sk) ? join(' ', @sk) : 'NONE')
	    . " ret_keys " . (scalar(@rk) ? join(' ', @rk) : 'NONE')
	    . " nb_sock " . ($nb_sock // 'undef')
            . "\n";
    }

    my $reading = {}; # bool
    my $writing = {}; # bool
    my $state   = {}; # 0 = waiting for a line, N = reading N bytes
    my $buf     = {}; # buffers
    my $offset  = {}; # offsets to read into buffers
    my $key     = {}; # current key per socket
    my $s_keys  = {}; # all keys being fetched for this socket
    my $flags   = {}; # flags per socket
    my $active_changed; # to force rebuild of select sets in blocking mode

    # retrieve values in case we're in nonblocking mode
    $reading = $self->{'load_multi_reading'} ||= {};
    $writing = $self->{'load_multi_writing'} ||= {};
    $state   = $self->{'load_multi_state'}   ||= {};
    $buf     = $self->{'load_multi_buf'}     ||= {};
    $offset  = $self->{'load_multi_offset'}  ||= {};
    $key     = $self->{'load_multi_key'}     ||= {};
    $s_keys  = $self->{'load_multi_s_keys'}  ||= {};
    $flags   = $self->{'load_multi_flags'}   ||= {};
    $ret     = $self->{'load_multi_ret'}     ||= {}
      unless ref $ret;
    my $dead     = $self->{'load_multi_sub_dead'};
    my $read     = $self->{'load_multi_sub_read'};
    my $write    = $self->{'load_multi_sub_write'};
    my $finalize = $self->{'load_multi_sub_finalize'};

    # clear these values for each socket unless nb_sock passed
    unless($nb_sock) {
	print STDERR __PACKAGE__ . "::_load_multi clear state because nb_sock not passed in\n" if Epolld::Settings::DEBUG;
      foreach my $s (keys %$sock_keys) {
        delete $self->{'load_multi_reading'}->{$s}; 
        delete $self->{'load_multi_writing'}->{$s}; 
        delete $self->{'load_multi_state'}->{$s};
        delete $self->{'load_multi_buf'}->{$s};
        delete $self->{'load_multi_offset'}->{$s};
        delete $self->{'load_multi_key'}->{$s};
        delete $self->{'load_multi_s_keys'}->{$s};
        delete $self->{'load_multi_flags'}->{$s};
        delete $self->{'load_multi_ret'}->{$s};

        print STDERR __PACKAGE__ . "::_load_multi watch write\n" if ($self->{'debug'} >= 2 || Epolld::Settings::DEBUG);
        $writing->{$s} = 1;
        $self->{'write_socks'}->{$s} = $s;
        $self->{'connection_module'}->sock_watch(
          $self->{'socks_for_crazy'}->{$s},0,1)
          if($self->{'connection_module'});
        $buf->{$s} = "get ". join(" ", map { "$self->{namespace}$_" } @{$sock_keys->{$s}}) . "\r\n";
        map { $s_keys->{$s}->{$_} = 1 } @{$sock_keys->{$s}}; 
      }
    }

    # in nonblocking mode, the first time through, this is enough
    if($self->{'nb'} && !$nb_sock) {
	print STDERR __PACKAGE__ . "::_load_multi nb mod, returning without loading anything\n" if Epolld::Settings::DEBUG;
	return;
    }

    $dead = $self->{'load_multi_sub_dead'} = sub {
        my $sock = shift;
        print STDERR __PACKAGE__ . "::_load_multi dead key=" . ($key->{$sock} // $key // 'undef') . " ($sock)\n";
        print STDERR __PACKAGE__ . "::_load_multi killing socket $sock\n" if ($self->{'debug'} >= 2 || Epolld::Settings::DEBUG);
        delete $reading->{$sock};
        delete $writing->{$sock};
        delete $self->{'read_socks'}->{$sock};
        delete $self->{'write_socks'}->{$sock};
        delete $self->{'stuck_socks'}->{$sock};
        $self->{'connection_module'}->sock_delete(
          $self->{'socks_for_crazy'}->{$sock})
          if($self->{'connection_module'});
        $self->{'prev_sockmode'}->{$sock} = $self->{'sockmode'}->{$sock}
          if($self->{'sockmode'}->{$sock});
        $self->{'sockmode'}->{$sock} = undef;
        delete $ret->{$key->{$sock}}
            if $key->{$sock};

        if ($self->{'stat_callback'}) {
            my $etime = Time::HiRes::time();
            $self->{'stat_callback'}->($self->{'_stime'}, $etime, $sock, 'load_multi');
        }

        close $sock;
        _dead_sock($sock);
        $active_changed = 1;
    }
    unless defined $dead;

    $finalize = $self->{'load_multi_sub_finalize'} = sub {
        my $sock = shift;
        my $k = $key->{$sock};
       
        my @responses;

        # remove trailing \r\n
        chop $ret->{$k}; chop $ret->{$k};

        unless (int(length($ret->{$k})) == $state->{$sock}-2) {
            $dead->($sock);
            return;
        }

        $ret->{$k} = Compress::Zlib::memGunzip($ret->{$k})
            if $HAVE_ZLIB && $flags->{$sock} & F_COMPRESS;
        if ($flags->{$sock} & F_STORABLE) {
            # wrapped in eval in case a perl 5.6 Storable tries to
            # unthaw data from a perl 5.8 Storable.  (5.6 is stupid
            # and dies if the version number changes at all.  in 5.8
            # they made it only die if it unencounters a new feature)
            eval {
                $ret->{$k} = Data::Storable::thaw($ret->{$k});
            };
            # so if there was a problem, try oldskool storable
            if($@) {
                eval {
                    $ret->{$k} = Storable::thaw($ret->{$k});
                };
                # now if there was a problem, just treat it as a cache miss.
                if ($@) {
                    delete $ret->{$k};
                }
            }
        }
        
        # now, if it was a hit, remove it from s_keys list
        if($ret->{$k}) {
          push @responses, {
            type  => 'HIT',
            key   => $k,
            value => ref($ret->{$k})? $ret->{$k}: \$ret->{$k},
          };
        }

        # otherwise, it is a miss
        else {
          push @responses, {
            type => 'MISS',
            key  => $k,
          };
        }

        # clean up
        delete $s_keys->{$sock}->{$k};
        delete $ret->{$k};

        if($self->{'nb'}) {
          return @responses;
        }
        else {
          return;
        }
    }
    unless defined $finalize;

    $read = $self->{'load_multi_sub_read'} = sub {
        my $sock = shift;
        my $res;
        my @responses;

        # where are we reading into?
        if ($state->{$sock}) { # reading value into $ret
            $res = sysread($sock, $ret->{$key->{$sock}},
                $state->{$sock} - $offset->{$sock},
                $offset->{$sock});
            return
                if !defined($res) and $!==EWOULDBLOCK;
            if ($res == 0) { # catches 0=conn closed or undef=error
                $dead->($sock);
                return;
            }
            $offset->{$sock} += $res;

            # finished reading
            if ($offset->{$sock} == $state->{$sock}) { 
                push @responses, $finalize->($sock);
                # wait for another VALUE line or END
                $state->{$sock} = 0; 
                $offset->{$sock} = 0;
            }
            return $self->{'nb'}? @responses: undef;
        }

        # we're reading a single line.
        # first, read whatever's there, but be satisfied with 2048 bytes
        $res = sysread($sock, $buf->{$sock},
                       2048, $offset->{$sock});
        return
            if !defined($res) and $!==EWOULDBLOCK;
        if ($res == 0) {
            $dead->($sock);
            return;
        }
        $offset->{$sock} += $res;

      SEARCH:
        while(1) { # may have to search many times
            # do we have a complete END line? 
            if ($buf->{$sock} =~ /^END\r\n/o) {
                # okay, finished with this socket
                delete $reading->{$sock};
                delete $self->{'read_socks'}->{$sock};
                delete $self->{'write_socks'}->{$sock};
		print STDERR __PACKAGE__ . "::_load_multi found END, unwatch\n" if Epolld::Settings::DEBUG;
                $self->{'connection_module'}->sock_watch(
                  $self->{'socks_for_crazy'}->{$sock},0,0)
                  if($self->{'connection_module'});
                $self->{'prev_sockmode'}->{$sock} =
                  $self->{'sockmode'}->{$sock}
                    if($self->{'sockmode'}->{$sock});
                $self->{'sockmode'}->{$sock} = undef;
                $active_changed = 1;
                if($self->{'nb'}) {
                  foreach my $miss_key (keys %{$s_keys->{$sock}}) {
                    push @responses, {
                      type => 'MISS',
                      key  => $miss_key,
                    };
                    delete $s_keys->{$sock}->{$miss_key};
                  }
                  return @responses;
                }
                else{
                  return;
                }
            }

            # do we have a complete VALUE line?
            if ($buf->{$sock} =~ /^VALUE (\S+) (\d+) (\d+)\r\n/o) {
                ($key->{$sock}, $flags->{$sock}, 
                 $state->{$sock}) =
                    (substr($1, $self->{namespace_len}), int($2), $3+2);
                # Note: we use $+[0] and not pos($buf->{$sock}) because pos()
                # seems to have problems under perl's taint mode.  nobody
                # on the list discovered why, but this seems a reasonable
                # work-around:
                my $p = $+[0];
                my $len = int(length($buf->{$sock}));
                my $copy = $len-$p > $state->{$sock}? $state->{$sock}: $len-$p;
                $ret->{$key->{$sock}} = substr($buf->{$sock}, $p, $copy)
                    if $copy;
                $offset->{$sock} = $copy;
                substr($buf->{$sock}, 0, $p+$copy, ''); # delete the stuff we used
                # have it all?
                if ($offset->{$sock} == $state->{$sock}) { 
                    push @responses, $finalize->($sock);
                    # wait for another VALUE line or END
                    $state->{$sock} = 0; 
                    $offset->{$sock} = 0;
                    next SEARCH; # look again
                }
                last SEARCH; # buffer is empty now
            }

            # if we're here probably means we only have a partial VALUE
            # or END line in the buffer. Could happen with multi-get,
            # though probably very rarely. Exit the loop and let it read
            # more.

            # but first, make sure subsequent reads don't destroy our
            # partial VALUE/END line.
            $offset->{$sock} = int(length($buf->{$sock}));
            last SEARCH;
        }

        # we don't have a complete line, wait and read more when ready
        if($self->{'nb'}) {
          return @responses;
        }
        else {
          return;
        }
    }
    unless defined $read;

    $write = $self->{'load_multi_sub_write'} = sub {
        my $sock = shift;
        my $res;

        $res = send($sock, $buf->{$sock}, $FLAG_NOSIGNAL);
        return
            if not defined $res and $!==EWOULDBLOCK;
        unless ($res > 0) {
            $dead->($sock);
            return;
        }
        if ($res == int(length($buf->{$sock}))) { # all sent
            $buf->{$sock} = "";
            $offset->{$sock} = $state->{$sock} = 0;
            # switch the socket from writing state to reading state
            delete $writing->{$sock};
            $reading->{$sock} = 1;
            delete $self->{'write_socks'}->{$sock};
            $self->{'read_socks'}->{$sock} = $sock;
	    print STDERR __PACKAGE__ . "::_load_multi finished write, watch read\n" if Epolld::Settings::DEBUG;
            $self->{'connection_module'}->sock_watch(
              $self->{'socks_for_crazy'}->{$sock},1,0)
              if($self->{'connection_module'});
            $active_changed = 1;
        } else { # we only succeeded in sending some of it
            substr($buf->{$sock}, 0, $res, ''); # delete the part we sent
        }
        return;
    }
    unless defined $write;

    $active_changed = 1; # force rebuilding of select sets

    if($self->{'nb'}) {             # non-blocking mode

	print STDERR __PACKAGE__ . "::_load_multi nb checking sockets\n" if Epolld::Settings::DEBUG;

      my @responses;

      # process read or write-readiness and gather responses
      if($writing->{$nb_sock}) {
        push @responses, $write->($nb_sock);
      }
      if($reading->{$nb_sock}) {
        push @responses, $read->($nb_sock);
      }
  
      # update socket state
      $self->{'read_socks'}->{$nb_sock} = $nb_sock
        if($reading->{$nb_sock});
      delete $self->{'read_socks'}->{$nb_sock}
        unless($reading->{$nb_sock});
      $self->{'write_socks'}->{$nb_sock} = $nb_sock
        if($writing->{$nb_sock});
      delete $self->{'write_socks'}->{$nb_sock}
        unless($writing->{$nb_sock});
	print STDERR __PACKAGE__ . "::_load_multi calling sock_watch\n" if Epolld::Settings::DEBUG;
      $self->{'connection_module'}->sock_watch(
        $self->{'socks_for_crazy'}->{$nb_sock},
        $reading->{$nb_sock}, $writing->{$nb_sock})
        if($self->{'connection_module'});

      # return any new $res values
      return @responses;
    }

    else {                          # blocking mode

	print STDERR __PACKAGE__ . "::_load_multi blocking select loop\n" if Epolld::Settings::DEBUG;

      # the bitsets for select
      my ($rin, $rout, $win, $wout);
      my $nfound;

      # the big select loop
      while(1) {
          if ($active_changed) {
              # no sockets left?
              last unless %$reading or %$writing; 

              ($rin, $win) = ('', '');
              foreach (keys %$reading) {
                  vec($rin, fileno($_), 1) = 1;
              }
              foreach (keys %$writing) {
                  vec($win, fileno($_), 1) = 1;
              }
              $active_changed = 0;
          }
          # TODO: more intelligent cumulative timeout?
          $nfound = select($rout=$rin, $wout=$win, undef,
                           $self->{'select_timeout'});
          last unless $nfound;

          # TODO: possible robustness improvement: we could select
          # writing sockets for reading also, and raise hell if they're
          # ready (input unread from last time, etc.)
          # maybe do that on the first loop only?
          foreach (keys %$writing) {
              if (vec($wout, fileno($_), 1)) {
                  $write->($_);
              }
          }
          foreach (keys %$reading) {
              if (vec($rout, fileno($_), 1)) {
                  $read->($_);
              }
          }
      }

      # if there're active sockets left, they need to die
      foreach (keys %$writing) {
          $dead->($_);
      }
      foreach (keys %$reading) {
          $dead->($_);
      }

    }

    return;
}

sub _hashfunc {
    return (crc32(shift) >> 16) & 0x7fff;
}

# returns array of lines, or () on failure.
sub run_command {
    #TODO nonblocking treatment?
    my Cache::NBMemcached $self = shift;
    my ($sock, $cmd) = @_;
    return () unless $sock;
    my $ret;
    my $line = $cmd;
    while (my $res = _oneline($self, $sock, \$line)) {
        undef $line;
    $ret .= $res;
        last if $ret =~ /(?:END|ERROR)\r\n$/;
    }
    chop $ret; chop $ret;
    return map { "$_\r\n" } split(/\r\n/, $ret);
}

sub stats {
    #TODO nonblocking treatment?
    my Cache::NBMemcached $self = shift;
    my ($types) = @_;
    return 0 unless $self->{'active'};
    return 0 unless !ref($types) || ref($types) eq 'ARRAY';
    if (!ref($types)) {
        if (!$types) {
            # I don't much care what the default is, it should just
            # be something reasonable.  Obviously "reset" should not
            # be on the list :) but other types that might go in here
            # include maps, cachedump, slabs, or items.
            $types = [ qw( misc malloc sizes self ) ];
        } else {
            $types = [ $types ];
        }
    }

    $self->init_buckets() unless $self->{'buckets'};

    my $stats_hr = { };

    # The "self" stat type is special, it only applies to this very
    # object.
    if (grep /^self$/, @$types) {
        $stats_hr->{'self'} = \%{ $self->{'stats'} };
    }

    # Now handle the other types, passing each type to each host server.
    my @hosts = @{$self->{'buckets'}};
    my %malloc_keys = ( );
  HOST: foreach my $host (@hosts) {
        my $sock = $self->sock_to_host($host);
      TYPE: foreach my $typename (grep !/^self$/, @$types) {
            my $type = $typename eq 'misc' ? "" : " $typename";
            my $l = "stats$type\r\n";
            my $line = _oneline($self, $sock, \$l);
            if (!$line) {
                _dead_sock($sock);
                next HOST;
            }

            # Some stats are key-value, some are not.  malloc,
            # sizes, and the empty string are key-value.
            # ("self" was handled separately above.)
            if ($typename =~ /^(malloc|sizes|misc)$/) {
                # This stat is key-value.
              LINE: while ($line) {
                    # We have to munge this data a little.  First, I'm not
                    # sure why, but 'stats sizes' output begins with NUL.
                    $line =~ s/^\0//;

                    # And, most lines end in \r\n but 'stats maps' (as of
                    # July 2003 at least) ends in \n.  An alternative
                    # would be { local $/="\r\n"; chomp } but this works
                    # just as well:
                    $line =~ s/[\r\n]+$//;

                    # OK, process the data until the end, converting it
                    # into its key-value pairs.
                    last LINE if $line eq 'END';
                    my($key, $value) = $line =~ /^(?:STAT )?(\w+)\s(.*)/;
                    if ($key) {
                        $stats_hr->{'hosts'}{$host}{$typename}{$key} = $value;
                    }
                    $malloc_keys{$key} = 1 if $typename eq 'malloc';

                    # read the next line
                    $line = _oneline($self, $sock);
                }
            } else {
                # This stat is not key-value so just pull it
                # all out in one blob.
              LINE: while ($line) {
                    $line =~ s/[\r\n]+$//;
                    last LINE if $line eq 'END';
                    $stats_hr->{'hosts'}{$host}{$typename} ||= "";
                    $stats_hr->{'hosts'}{$host}{$typename} .= "$line\n";

                    # read the next one
                    $line = _oneline($self, $sock);
                }
            }
        }
    }

    # Now get the sum total of applicable values.  First the misc values.
    foreach my $stat (qw(
        bytes bytes_read bytes_written
        cmd_get cmd_set connection_structures curr_items
        get_hits get_misses
        total_connections total_items
        )) {
        $stats_hr->{'total'}{$stat} = 0;
        foreach my $host (@hosts) {
            $stats_hr->{'total'}{$stat} +=
                $stats_hr->{'hosts'}{$host}{'misc'}{$stat};
        }
    }

    # Then all the malloc values, if any.
    foreach my $malloc_stat (keys %malloc_keys) {
        $stats_hr->{'total'}{"malloc_$malloc_stat"} = 0;
        foreach my $host (@hosts) {
            $stats_hr->{'total'}{"malloc_$malloc_stat"} +=
                $stats_hr->{'hosts'}{$host}{'malloc'}{$malloc_stat};
        }
    }

    return $stats_hr;
}

sub stats_reset {
    #TODO nonblocking treatment?
    my Cache::NBMemcached $self = shift;
    my ($types) = @_;
    return 0 unless $self->{'active'};

    $self->init_buckets() unless $self->{'buckets'};

  HOST: foreach my $host (@{$self->{'buckets'}}) {
        my $sock = $self->sock_to_host($host);
        my $l = "stats reset";
        my $ok = _oneline($self, $sock, \$l);
        unless ($ok eq "RESET\r\n") {
            _dead_sock($sock);
        }
    }
    return 1;
}


1;
__END__

=head1 NAME

Cache::Memcached - client library for memcached (memory cache daemon)

=head1 SYNOPSIS

  use Cache::Memcached;

  $memd = new Cache::Memcached {
    'servers' => [ "10.0.0.15:11211", "10.0.0.15:11212",
                   "10.0.0.17:11211", [ "10.0.0.17:11211", 3 ] ],
    'debug' => 0,
    'compress_threshold' => 10_000,
  };
  $memd->set_servers($array_ref);
  $memd->set_compress_threshold(10_000);
  $memd->enable_compress(0);

  $memd->set("my_key", "Some value");
  $memd->set("object_key", { 'complex' => [ "object", 2, 4 ]});

  $val = $memd->get("my_key");
  $val = $memd->get("object_key");
  if ($val) { print $val->{'complex'}->[2]; }

  $memd->incr("key");
  $memd->decr("key");
  $memd->incr("key", 2);

=head1 DESCRIPTION

This is the Perl API for memcached, a distributed memory cache daemon.
More information is available at:

  http://www.danga.com/memcached/

=head1 CONSTRUCTOR

=over 4

=item C<new>

Takes one parameter, a hashref of options.  The most important key is
C<servers>, but that can also be set later with the C<set_servers>
method.  The servers must be an arrayref of hosts, each of which is
either a scalar of the form C<10.0.0.10:11211> or an arrayref of the
former and an integer weight value.  (The default weight if
unspecified is 1.)  It's recommended that weight values be kept as low
as possible, as this module currently allocates memory for bucket
distribution proportional to the total host weights.

Use C<compress_threshold> to set a compression threshold, in bytes.
Values larger than this threshold will be compressed by C<set> and
decompressed by C<get>.

Use C<no_rehash> to disable finding a new memcached server when one
goes down.  Your application may or may not need this, depending on
your expirations and key usage.

Use C<readonly> to disable writes to backend memcached servers.  Only
get and get_multi will work.  This is useful in bizarre debug and
profiling cases only.

The other useful key is C<debug>, which when set to true will produce
diagnostics on STDERR.

=back

=head1 METHODS

=over 4

=item C<set_servers>

Sets the server list this module distributes key gets and sets between.
The format is an arrayref of identical form as described in the C<new>
constructor.

=item C<set_debug>

Sets the C<debug> flag.  See C<new> constructor for more information.

=item C<set_readonly>

Sets the C<readonly> flag.  See C<new> constructor for more information.

=item C<set_norehash>

Sets the C<no_rehash> flag.  See C<new> constructor for more information.

=item C<set_compress_threshold>

Sets the compression threshold. See C<new> constructor for more information.

=item C<enable_compress>

Temporarily enable or disable compression.  Has no effect if C<compress_threshold>
isn't set, but has an overriding effect if it is.

=item C<get>

my $val = $memd->get($key);

Retrieves a key from the memcache.  Returns the value (automatically
thawed with Storable, if necessary) or undef.

The $key can optionally be an arrayref, with the first element being the
hash value, if you want to avoid making this module calculate a hash
value.  You may prefer, for example, to keep all of a given user's
objects on the same memcache server, so you could use the user's
unique id as the hash value.

=item C<get_multi>

my $hashref = $memd->get_multi(@keys);

Retrieves multiple keys from the memcache doing just one query.
Returns a hashref of key/value pairs that were available.

This method is recommended over regular 'get' as it lowers the number
of total packets flying around your network, reducing total latency,
since your app doesn't have to wait for each round-trip of 'get'
before sending the next one.

=item C<set>

$memd->set($key, $value[, $exptime]);

Unconditionally sets a key to a given value in the memcache.  Returns true
if it was stored successfully.

The $key can optionally be an arrayref, with the first element being the
hash value, as described above.

The $exptime (expiration time) defaults to "never" if unspecified.  If
you want the key to expire in memcached, pass an integer $exptime.  If
value is less than 60*60*24*30 (30 days), time is assumed to be relative
from the present.  If larger, it's considered an absolute Unix time.

=item C<add>

$memd->add($key, $value[, $exptime]);

Like C<set>, but only stores in memcache if the key doesn't already exist.

=item C<replace>

$memd->replace($key, $value[, $exptime]);

Like C<set>, but only stores in memcache if the key already exists.  The
opposite of C<add>.

=item C<delete>

$memd->delete($key[, $time]);

Deletes a key.  You may optionally provide an integer time value (in seconds) to
tell the memcached server to block new writes to this key for that many seconds.
(Sometimes useful as a hacky means to prevent races.)  Returns true if key
was found and deleted, and false otherwise.

=item C<incr>

$memd->incr($key[, $value]);

Sends a command to the server to atomically increment the value for
$key by $value, or by 1 if $value is undefined.  Returns undef if $key
doesn't exist on server, otherwise it returns the new value after
incrementing.  Value should be zero or greater.  Overflow on server
is not checked.  Be aware of values approaching 2**32.  See decr.

=item C<decr>

$memd->decr($key[, $value]);

Like incr, but decrements.  Unlike incr, underflow is checked and new
values are capped at 0.  If server value is 1, a decrement of 2
returns 0, not -1.

=item C<stats>

$memd->stats([$keys]);

Returns a hashref of statistical data regarding the memcache server(s),
the $memd object, or both.  $keys can be an arrayref of keys wanted, a
single key wanted, or absent (in which case the default value is malloc,
sizes, self, and the empty string).  These keys are the values passed
to the 'stats' command issued to the memcached server(s), except for
'self' which is internal to the $memd object.  Allowed values are:

=over 4

=item C<misc>

The stats returned by a 'stats' command:  pid, uptime, version,
bytes, get_hits, etc.

=item C<malloc>

The stats returned by a 'stats malloc':  total_alloc, arena_size, etc.

=item C<sizes>

The stats returned by a 'stats sizes'.

=item C<self>

The stats for the $memd object itself (a copy of $memd->{'stats'}).

=item C<maps>

The stats returned by a 'stats maps'.

=item C<cachedump>

The stats returned by a 'stats cachedump'.

=item C<slabs>

The stats returned by a 'stats slabs'.

=item C<items>

The stats returned by a 'stats items'.

=back

=item C<disconnect_all>

$memd->disconnect_all();

Closes all cached sockets to all memcached servers.  You must do this
if your program forks and the parent has used this module at all.
Otherwise the children will try to use cached sockets and they'll fight
(as children do) and garble the client/server protocol.

=back

=head1 BUGS

When a server goes down, this module does detect it, and re-hashes the
request to the remaining servers, but the way it does it isn't very
clean.  The result may be that it gives up during its rehashing and
refuses to get/set something it could've, had it been done right.

=head1 COPYRIGHT

This module is Copyright (c) 2003 Brad Fitzpatrick.
All rights reserved.

You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file.

=head1 WARRANTY

This is free software. IT COMES WITHOUT WARRANTY OF ANY KIND.

=head1 FAQ

See the memcached website:
   http://www.danga.com/memcached/

=head1 AUTHORS

Brad Fitzpatrick <brad@danga.com>

Anatoly Vorobey <mellon@pobox.com>

Brad Whitaker <whitaker@danga.com>

Jamie McCarthy <jamie@mccarthy.vg>
