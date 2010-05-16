
use strict;
use warnings;
package RiakFuse;
use threads;
use threads::shared;


use RiakFuse::Filepath;
use RiakFuse::Data;
use Data::Dumper;
use HTTP::Date;
our %params;
use POSIX qw(ENOTEMPTY ENOENT EEXIST EACCES);
my $fuse;
use RiakFuse::Stats;

use Fuse qw(:xattr fuse_get_context);


use URI::Escape;

our %servers : shared;
our @servers : shared;

sub conf {
    my %opts = @_;
    $params{mountopt}   = $opts{mountopt} || "";
    $params{mountpoint} = $opts{mountpoint} || die;
    $params{debug} =      $opts{debug} || 0;
    $params{threaded} =   0,
    $params{trace}    =   $opts{trace}    || 0;
    $params{bufferdir}  = $opts{bufferdir} || die;
    $params{filebucket} = $opts{filebucket} || die;
    $params{logbucket}  = $opts{logbucket} || die;
    $params{servers}    = $opts{servers} || die;


    if($params{threaded}) {
	threads->new(
	    sub {
		RiakFuse::Stats->start(\%params);
	    })->detach;
	
	{
	    while (keys %servers == 0) {
		print STDERR "Trying to connect to servers ( ". join(' ,', @{$params{servers}}) . " )\n";
		lock(%servers);
		cond_wait(%servers) if(keys %servers == 0);
	    }
	}
    } else {
	RiakFuse::HTTP->CLONE();
	foreach my $server (@{$params{servers}}) {
	    $servers{$server} = 1;
	}
    }


    #mountopts => "nolocalcaches" is needed to get sane behaviour on OSX
    #sadly Fuse.pm thinks it is invalid
    #also makes everything every very slow

    $fuse = Fuse::main(
	mountpoint => $params{mountpoint},
	mountopts => $params{mountopt},
	debug      => $params{debug},
	threaded   => $params{threaded},
	getattr => 'RiakFuse::my_getattr',
	statfs  => 'RiakFuse::my_statfs',
	getdir =>"RiakFuse::my_getdir",
	mknod  => "RiakFuse::my_mknod",
	utime => "RiakFuse::my_utime",
	write  => "RiakFuse::my_write",
	read   => "RiakFuse::my_read",
	truncate => "RiakFuse::my_truncate",
	open   =>"RiakFuse::my_open",
	mkdir => "RiakFuse::my_mkdir",
	unlink => "RiakFuse::my_unlink",
	rmdir => "RiakFuse::my_rmdir",
	rename => "RiakFuse::my_rename",
	chmod => "RiakFuse::my_chmod",
	chown => "RiakFuse::my_chown",
	flush => "RiakFuse::my_release",
	release => "RiakFuse::my_flush",
	setxattr => "RiakFuse::my_setxattr",
	getxattr => "RiakFuse::my_getxattr",
	);
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
    my @active_servers;
    foreach my $server (keys %servers) {
	push @active_servers, $server if($servers{$server});
    }
    $last_server = $active_servers[rand @active_servers];
    return $last_server;
}

sub my_setxattr {
    my $file = RiakFuse::Filepath->new(shift());
    my $xattr_key   = shift;
    my $xattr_value = shift;
    my $flags = shift;
    RiakFuse::Stats->increment("setxattr");
    # check flags here;
    
    print "> setxattr (".$file->orig.") -> ($xattr_key = $xattr_value => $flags)\n" if($params{trace} > 1);


    my $parent = RiakFuse::Data->get($file->parent);
    return -ENOENT() unless ref $parent;
    return -ENOENT() unless exists $parent->{content}->{$file->name};
    $parent->{content}->{$file->name}->{xattr}->{$xattr_key} = $xattr_value;
    RiakFuse::Data->put($file->parent, "application/json", $parent->{content});
    return 0;
}

sub my_getxattr {
    my $file = RiakFuse::Filepath->new(shift());
    my $xattr = shift;
    print "> getxattr (".$file->orig.") -> ($xattr)\n" if($params{trace} > 1);
    RiakFuse::Stats->increment("getxattr");
    my $parent = RiakFuse::Data->get($file->parent);

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

    my $parent = RiakFuse::Data->get($file->parent);

    return -ENOENT() unless ref $parent;
    return -ENOENT() unless exists $parent->{content}->{$file->name};

    $parent->{content}->{$file->name}->{uid} = $uid;
    $parent->{content}->{$file->name}->{gid} = $gid;

    RiakFuse::Data->put($file->parent, "application/json", $parent->{content});
    return 0;
}

sub my_release {
    my $file = RiakFuse::Filepath->new(shift());
    print "> release (".$file->orig.")\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("release");
    return 0;
}

sub my_flush {
    my $file = RiakFuse::Filepath->new(shift());
    print "> flush (".$file->orig.")\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("flush");
    return 0;
}


sub my_rename {
    my $old = RiakFuse::Filepath->new(shift());
    my $new = RiakFuse::Filepath->new(shift());
    print "> rname ( ".$old->orig ." -> ". $new->orig ." )\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("rename");
    my $old_parent = RiakFuse::Data->get($old->parent);
    
    if($old_parent->{content}->{$old->name}->{type} == 32) {
	return -EACCES();
    }

    my $data = RiakFuse::Data->get($old);
    RiakFuse::Data->put($new, $data->{'content-type'}, $data->{content});

    my $content;
    {
	my $dir = RiakFuse::Data->get($old->parent);
	$content = delete($dir->{content}->{$old->name});
	RiakFuse::Data->put($old->parent, "application/json", $dir->{content});
    }
    {
	my $dir = RiakFuse::Data->get($new->parent);
	$dir->{content}->{$new->name} = $content;
	RiakFuse::Data->put($new->parent, "application/json", $dir->{content});
    }
    RiakFuse::Data->delete($old);
}

sub my_rmdir {
    my $file = RiakFuse::Filepath->new(shift());
    print "> rmdir ($file->{orig})\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("rmdir");
    my $obj = RiakFuse::Data->get($file);
    if(ref($obj->{content}) && keys %{$obj->{content}} > 2) {
	return -ENOTEMPTY();
    }

    my $dir = RiakFuse::Data->get($file->parent);
    delete ($dir->{content}->{$file->name});
    RiakFuse::Data->put($file->parent, "application/json", $dir->{content});
    RiakFuse::Data->delete($file);
    return 0;
}

sub my_unlink {
    my $file = RiakFuse::Filepath->new(shift());
    print "> unlink ($file->{orig})\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("unlink");
    my $dir = RiakFuse::Data->get($file->parent);
    delete($dir->{content}->{$file->name});
    RiakFuse::Data->put($file->parent, "application/json", $dir->{content});
    RiakFuse::Data->delete($file);
}

sub my_mkdir {
    my $file = RiakFuse::Filepath->new(shift());
    print "> mkdir " . $file->key . "\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("mkdir");
    my $mode = shift;
    my $type = $mode >> 9;
    $mode = ($mode - ($type << 9));

    my $parent = RiakFuse::Data->get($file->parent);
    
    if($parent->{content}->{$file->name}) {
	return -EEXIST();
    }

    RiakFuse::Data->put($file,
			'application/json',
			{
			    '.' => {},
			    '..' => {},
			});

    my %tmp;
    share(%tmp);

    %tmp = (
	atime => time,
	ctime => time,
	filename => $file->orig,
	mode => $mode,
	type => $type,
	uid  => fuse_get_context()->{"uid"},
	gid  => fuse_get_context()->{"gid"},
    );
    $parent->{content}->{$file->name} =\%tmp;

    RiakFuse::Data->put($file->parent, "application/json", $parent->{content});

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
	return 0;
    } else {
	return -ENOSYS;
    }
}

sub my_truncate {
    RiakFuse::Stats->increment("truncate");
    my $file = RiakFuse::Filepath->new(shift());
    print "> truncate " . $file->orig ."\n" if($params{trace} > 3);
    my $obj  = RiakFuse::Data->get($file);
    RiakFuse::Data->put($file, $obj->{'content-type'}, "");
    return 0;
}

sub my_read {
    print "> read\n" if($params{trace} > 3);
    my $file = RiakFuse::Filepath->new(shift());
    RiakFuse::Stats->increment("read");
    my $request_size = shift;
    my $offset = shift;
    return RiakFuse::Stats->stats_read($file, $request_size, $offset) if ($file->orig =~/^\/.riakfs/);

    my $content = RiakFuse::Data->get($file)->{content};
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
    my $obj = RiakFuse::Data->get($file);
    my $written = substr($obj->{content}, $offset, $len, $buffer);
    my $error = RiakFuse::Data->put($file, $obj->{"content-type"}, $obj->{content});
    return $error if($error != 0);
    return $len;
}


sub my_mknod {
    my $file = RiakFuse::Filepath->new(shift());
    print "> mknod ($file->{orig})\n" if($params{trace} > 3);
    RiakFuse::Stats->increment("mknod");
    my $mode = shift;
    my $dev = shift;
    my $type = $mode >> 9;
    $mode = ($mode - ($type << 9));
    
    my $node = RiakFuse::Data->get($file->parent);
    return $node unless ref $node;
    
    return -EEXIST() if (exists($node->{content}->{$file->name}));

    $node->{content}->{$file->name} = {
	atime => time,
	ctime => time,
	filename => $file->name,
	mode => $mode,
	type => $type,
	uid  => fuse_get_context()->{"uid"},
	gid  => fuse_get_context()->{"gid"},
    };

    RiakFuse::Data->put($file->parent, "application/json", $node->{content});

    RiakFuse::Data->put($file, "application/octect-stream", "");

    return 0;
}

sub my_getdir {
    print "> getdir\n" if($params{trace} > 3);
    my $file = RiakFuse::Filepath->new(shift());
    RiakFuse::Stats->increment("getdir");
    if($file->orig =~/^\/.riakfs/) {
	return (RiakFuse::Stats->getdir($file), 0);
    }

    my $obj = RiakFuse::Data->get($file,0);
    my @rv = map { uri_unescape($_) } keys %{$obj->{content}};

    #magic subdir full of stats

    if ($file->key eq '%2F') {
	push @rv,".riakfs";
    }
    return (@rv, 0);
}

sub my_statfs {
    print "> statfs\n" if($params{trace} > 6);
    RiakFuse::Stats->increment("statfs");
    return 255, 1, 1, 256*1024, 256*1024, 2;
}


sub my_getattr {
    my $file = RiakFuse::Filepath->new(shift());
    print "> getattr " . $file->orig . " (" . $file->key . ")\n"  if($params{trace} > 10);
    RiakFuse::Stats->increment("getattr");
    if ($file->orig =~/^\/.riakfs/) {
	return RiakFuse::Stats->getattr($file);
    }

    #XXX use head here
    my $node;

    my $stat;
    if($file->key eq '%2F') {
	$node = RiakFuse::Data->get($file,1);
	return $node unless ref $node;
	#we are root so our metadata is self contained
	$stat = $node->{content}->{'.'};
    } else {
	$node = RiakFuse::Data->head($file,1);
	return $node unless ref $node;
	my $parent = RiakFuse::Data->get($file->parent,1);
#	die unless ref $parent;
	$stat = $parent->{content}->{$file->name};
    }

    return (
	0,
	0,
	$stat->{mode} + ($stat->{type} <<9),
	1,
	$stat->{uid},
	$stat->{gid},
	0, #rdev
	$node->{'content-length'}, #size
	$stat->{atime},
	$node->{'last-modified'},
	$stat->{ctime},
	4096, #blksize
	1, #blocks
	);

}


1;
