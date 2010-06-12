
use strict;
use warnings;
package RiakFuse;
use threads;
use threads::shared;


use RiakFuse::Filepath;
use RiakFuse::Data;
use RiakFuse::MetaData::Directory;
use RiakFuse::MetaData::File;

use Data::Dumper;
use HTTP::Date;

use POSIX qw(ENOTEMPTY ENOENT EEXIST EACCES ENOTDIR EIO);
my $fuse;
use RiakFuse::Stats;

use Fuse qw(:xattr fuse_get_context);

use Time::HiRes qw(time);

my $timer;
sub start_timer {
    $timer = time;
}

sub stop_timer {
    my $duration = time - $timer;
    print STDERR ">>> Duration " . $duration . "\n";
}

use URI::Escape;

our %servers : shared;
our @servers : shared;
our %open_paths;
our $conf;
our %params = (trace => 0);
sub run {
    my $class = shift;
    $conf = shift;

	$fuse = Fuse::main(
	    mountpoint => $conf->{mountpoint},
	    mountopts => $conf->{mountopt},
	    debug      => $conf->{debug},
	    threaded   => $conf->{threaded},
	    getattr => 'RiakFuse::my_getattr',
	    statfs  => 'RiakFuse::my_statfs',
	    getdir =>"RiakFuse::my_getdir",
	    mknod  => "RiakFuse::my_mknod",
	    mkdir => "RiakFuse::my_mkdir",
	    rmdir => "RiakFuse::my_rmdir",
	    unlink => "RiakFuse::my_unlink",

#	    utime => "RiakFuse::my_utime",
#	    write  => "RiakFuse::my_write",
#	    read   => "RiakFuse::my_read",
#	    truncate => "RiakFuse::my_truncate",
#	    open   =>"RiakFuse::my_open",



	    rename => "RiakFuse::my_rename",
	    chmod => "RiakFuse::my_chmod",
	    chown => "RiakFuse::my_chown",
#	    flush => "RiakFuse::my_flush",
#	    release => "RiakFuse::my_release",
#	    setxattr => "RiakFuse::my_setxattr",
#	    getxattr => "RiakFuse::my_getxattr",
#	    );
	);
    
}

my $time = time();

sub house_cleaning {
  my $self = shift;
  
}

sub CLONE {
    RiakFuse::Stats->increment("threads");
}

my $last_server;
sub get_server {

    # prefer the previous server
    # since it is likely we have keep alive
    return $last_server if($last_server && $servers{$last_server});

    # XX deal with the case of no active servers
    for (1..5) {
	my @active_servers;
	foreach my $server (keys %servers) {
	    push @active_servers, $server if($servers{$server});
	}
	unless (@active_servers) {
	    sleep 1;
	    warn "No active servers found\n";
	    next;
	}
	$last_server = $active_servers[rand @active_servers];
	return ($last_server, 0);
    }
    return (undef, -EIO());
}

sub my_setxattr {
    my $file = RiakFuse::Filepath->new(shift());
    my $xattr_key   = shift;
    my $xattr_value = shift;
    my $flags = shift;
    RiakFuse::Stats->increment("setxattr");
    # check flags here;
    
    print "> setxattr (".$file->orig.") -> ($xattr_key = $xattr_value => $flags)\n" if($params{trace} > 1);

    for(1..5) {
	my $parent = RiakFuse::Data->get($file->parent);
	return -ENOENT() unless ref $parent;
	return -ENOENT() unless exists $parent->{content}->{$file->name};
	$parent->{content}->{$file->name}->{xattr}->{$xattr_key} = $xattr_value;
	$parent->{'if-match'} = $parent->{'etag'};
	my $rv = RiakFuse::Data->put($file->parent, $parent);
	next if($rv == 1); # retry
	return $rv if $rv < 0;
	return 0;
    }
    return -EIO();
}

sub my_getxattr {
    my $file = RiakFuse::Filepath->new(shift());
    my $xattr = shift;
    print "> getxattr (".$file->orig.") -> ($xattr)\n" if($params{trace} > 1);
    RiakFuse::Stats->increment("getxattr");
    my $parent = RiakFuse::Data->get($file->parent,1);

    return -ENOENT() unless ref $parent;
    return -ENOENT() unless exists $parent->{content}->{$file->name};

    return $parent->{content}->{$file->name}->{xattr}->{$xattr}
    if exists $parent->{content}->{$file->name}->{xattr}->{$xattr};
    return 0;
}

sub my_utime {
    my $file = RiakFuse::Filepath->new(shift());
    print "> utime (".$file->orig.")\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("utime");
    return 0;
}

sub my_chmod {
    my $file = RiakFuse::Filepath->new(shift());
    print "> chmod (".$file->orig.")\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("chmod");
    return 0;
}

sub my_chown {
    my $file = RiakFuse::Filepath->new(shift());
    print "> chown(".$file->orig.")\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("chown");
    my $uid = shift;
    my $gid = shift;

    my %args;

    $args{uid} = $uid if $uid > 0;
    $args{gid} = $gid if $gid > 0;

    my $entry = RiakFuse::MetaData->get($conf, $file);

    return $entry->{errno} if $entry->is_error;



    my $response = $entry->attr($conf, 
				%args,
	);
    return $response->{errno} if $response->is_error;
    return 0;
}

sub my_release {
    my $file = RiakFuse::Filepath->new(shift());
    print "> release (".$file->orig.")\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("release");
    if($open_paths{$file->key}) {
	$open_paths{$file->key}--;
	delete $open_paths{$file->key} unless $open_paths{$file->key};
    }
    return 0;
}

sub my_flush {
    my $file = RiakFuse::Filepath->new(shift());
    print "> flush (".$file->orig.")\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("flush");
    if($open_paths{$file->key}) {
	my $obj = RiakFuse::Data->get($file,1);
	RiakFuse::Data->put($file,$obj);
    }
    return 0;
}



# XXXX this doesn't guard for conflicts
sub my_rename {
    my $old = RiakFuse::Filepath->new(shift());
    my $new = RiakFuse::Filepath->new(shift());
    return -EIO();
}

sub my_rmdir {
    my $file = RiakFuse::Filepath->new(shift());
    print "> rmdir ($file->{orig})\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("rmdir");

    my $entry = RiakFuse::MetaData->get($conf, $file);

    return $entry->errno if ($entry->is_error);

    return -ENOTDIR() unless $entry->is_directory;
    return -ENOTEMPTY() if @{$entry->children};

    my $parent = RiakFuse::MetaData->get($conf, $file->parent);
    $parent->remove_child($conf, $entry);

    return 0;
}

sub my_unlink {
    my $file = RiakFuse::Filepath->new(shift());
    print "> unlink ($file->{orig})\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("unlink");
    
    my $entry = RiakFuse::MetaData->get($conf, $file);
    return $entry->errno if ($entry->is_error);
    return -EIO() if($entry->is_directory);
    my $parent = RiakFuse::MetaData->get($conf, $file->parent);
    $parent->remove_child($conf, $entry);

    return 0;
}




sub my_open {
    my $file = RiakFuse::Filepath->new(shift());
    print "> open $file->{orig}\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("open");

    return RiakFuse::Stats->stats_open($file) if ($file->orig =~/^\/.riakfs/);

    my $flags = shift;
    my $obj = RiakFuse::Data->get($file);
    if(ref($obj)) {
	$open_paths{$file->key}++;
	return 0;
    } else {
	return $obj;
    }
}

sub my_truncate {
    RiakFuse::Stats->increment("truncate");
    my $file = RiakFuse::Filepath->new(shift());
    my $offset = shift;
    print "> truncate " . $file->orig ." at $offset\n" if($params{trace} > 3);
    my $obj  = RiakFuse::Data->get($file);
    my $len = length($obj->{content});
    substr($obj->{content}, $offset, $len - $offset, "");
    RiakFuse::Data->put($file, $obj);
    return 0;
}

sub my_read {
    print "> read\n" if($params{trace} > 3);
    my $file = RiakFuse::Filepath->new(shift());
    RiakFuse::Stats->increment("read");
    my $request_size = shift;
    my $offset = shift;
    return RiakFuse::Stats->stats_read($file, $request_size, $offset) if ($file->orig =~/^\/.riakfs/);

    my $content = RiakFuse::Data->get($file,1)->{content};
    print "> read $request_size att offset $offset from file " . $file->key . "\n"  if($params{trace} > 3);

    return substr($content, $offset, $request_size);
}


sub my_write {
    my $file = RiakFuse::Filepath->new(shift());
    print "> write ($file->{orig})\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("write");
    my $buffer = shift;
    my $offset = shift;
    my $len = length($buffer);
    my $obj = RiakFuse::Data->get($file,1);
    return $obj unless ref $obj;
    my $written = substr($obj->{content}, $offset, $len, $buffer);
    unless ($open_paths{$file->key}) {
	my $error = RiakFuse::Data->put($file, $obj);
	return $error if($error != 0);
    }
    return $len;
}


sub my_mkdir {
    my $file = RiakFuse::Filepath->new(shift());
    print "> mkdir " . $file->key . "\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("mkdir");
    my $mode = shift;
    my $type = $mode >> 9;
    $mode = ($mode - ($type << 9));


    my $exists = RiakFuse::MetaData->get($conf, $file);

    if (!$exists->is_error) {
      return -EEXIST();
    } elsif ($exists->is_error && $exists->{errno} != -ENOENT()) {
      return $exists->{errno};
    }

    my $entry = RiakFuse::MetaData::Directory->new(
					     path => $file,
					     gid  => fuse_get_context()->{"gid"},
					     uid  => fuse_get_context()->{"uid"},
					     mode => $mode,
					     type => $type,
					    );

    my $parent = RiakFuse::MetaData->get($conf, $file->parent);
    $parent->add_child($conf, $entry);
    return 0;
}


sub my_mknod {
    my $file = RiakFuse::Filepath->new(shift());
#    print "> mknod ($file->{orig})\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("mknod");
    my $mode = shift;
    my $dev = shift;
    my $type = $mode >> 9;
    $mode = ($mode - ($type << 9));


    my $exists = RiakFuse::MetaData->get($conf, $file);

    if (!$exists->is_error) {
      return -EEXIST();
    } elsif ($exists->is_error && $exists->{errno} != -ENOENT()) {
      return $exists->{errno};
    }

    my $entry = RiakFuse::MetaData::File->new(
					     path => $file,
					     gid  => fuse_get_context()->{"gid"},
					     uid  => fuse_get_context()->{"uid"},
					     mode => $mode,
					     type => $type,
					    );

    my $parent = RiakFuse::MetaData->get($conf, $file->parent);

    $parent->add_child($conf, $entry);
    return 0;
}

sub my_getdir {
    print "> getdir\n" if($params{trace} > 3);
    my $file = RiakFuse::Filepath->new(shift());
    RiakFuse::Stats->increment("getdir");

    my $entry = RiakFuse::MetaData->get($conf, $file);

    return (@{$entry->children}, 0);

}

sub my_statfs {
#    print "> statfs\n" if($params{trace} > 6);
    RiakFuse::Stats->increment("statfs");
    return 255, 1, 1, 256*1024, 256*1024, 2;
}


sub my_getattr {
    my $file = RiakFuse::Filepath->new(shift());
    RiakFuse::Stats->increment("getattr");
#    if ($file->orig =~/^\/.riakfs/) {
#	#return RiakFuse::Stats->getattr($file);
#    }


    my $entry = RiakFuse::MetaData->get($conf, $file);

    if ($entry->is_error) {
      return $entry->{errno};
    }

    return (
	0,
	0,
	$entry->{mode} + ($entry->{type} <<9),
	1,
	$entry->{uid},
	$entry->{gid},
	0, #rdev
	$entry->{'size'}, #size
	time,
	$entry->{'mtime'},
	$entry->{'ctime'},
	4096, #blksize
	1, #blocks
	);

}


1;
