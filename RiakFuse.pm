
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

use Fuse qw(:xattr fuse_get_context);


use URI::Escape;

sub conf {
    my %opts = @_;
    $params{mountpoint} = $opts{mountpoint} || die;
    $params{debug} =      $opts{debug} || 0;
    $params{threaded} =   1,
    $params{trace}    =   $opts{trace}    || 0;
    $params{bufferdir}  = $opts{bufferdir} || die;
    $params{filebucket} = $opts{filebucket} || die;
    $params{logbucket}  = $opts{logbucket} || die;
    $params{server}     = $opts{server} || die;

    $fuse = Fuse::main(
	mountpoint => $params{mountpoint},
	mountopts => "nolocalcaches",
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

sub my_setxattr {
    my $file = RiakFuse::Filepath->new(shift());
    my $xattr_key   = shift;
    my $xattr_value = shift;
    my $flags = shift;

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
    return 0;
}

sub my_chmod {
    my $file = RiakFuse::Filepath->new(shift());
    print "> chmod (".$file->orig.")\n" if($params{trace} > 3);
    return 0;
}

sub my_chown {
    my $file = RiakFuse::Filepath->new(shift());
    print "> chown(".$file->orig.")\n" if($params{trace} > 3);
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
    return 0;
}

sub my_flush {
    my $file = RiakFuse::Filepath->new(shift());
    print "> flush (".$file->orig.")\n" if($params{trace} > 3);
    return 0;
}


sub my_rename {
    my $old = RiakFuse::Filepath->new(shift());
    my $new = RiakFuse::Filepath->new(shift());
    
    
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
    my $dir = RiakFuse::Data->get($file->parent);
    delete($dir->{content}->{$file->name});
    RiakFuse::Data->put($file->parent, "application/json", $dir->{content});
    RiakFuse::Data->delete($file);
}

sub my_mkdir {
    my $file = RiakFuse::Filepath->new(shift());
    print "> mkdir " . $file->key . "\n" if($params{trace} > 3);
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

    $parent->{content}->{$file->name} = {
			    atime => time,
			    ctime => time,
			    filename => $file->orig,
			    mode => $mode,
			    type => $type,
			    uid  => fuse_get_context()->{"uid"},
			    gid  => fuse_get_context()->{"gid"},
    };


    RiakFuse::Data->put($file->parent, "application/json", $parent->{content});

    return 0;
}


sub my_open {
    print "> open\n" if($params{trace} > 3);
    my $file = RiakFuse::Filepath->new(shift());
    my $flags = shift;
    my $obj = RiakFuse::Data->get($file);
    if(ref($obj)) {
	return 0;
    } else {
	return -ENOSYS;
    }
}

sub my_truncate {
    print "> truncate\n" if($params{trace} > 3);
    my $file = RiakFuse::Filepath->new(shift());
    my $obj  = RiakFuse::Data->get($file);
    RiakFuse::Data->put($file, $obj->{'content-type'}, "");
    return 0;
}

sub my_read {
    print "> read\n" if($params{trace} > 3);
    my $file = RiakFuse::Filepath->new(shift());
    my $request_size = shift;
    my $offset = shift;
    my $content = RiakFuse::Data->get($file)->{content};
    print "> read $request_size att offset $offset from file " . $file->key . "\n"  if($params{trace} > 3);
    
    return substr($content, $offset, $request_size);
}


sub my_write {
    my $file = RiakFuse::Filepath->new(shift());
    print "> write ($file->{orig})\n" if($params{trace} > 3);
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
    my $obj = RiakFuse::Data->get($file);
    return (map { uri_unescape($_) } keys %{$obj->{content}}, 0);
}

sub my_statfs {
    print "> statfs\n" if($params{trace} > 6);
    return 255, 1, 1, 256*1024, 256*1024, 2;
}


sub my_getattr {
    my $file = RiakFuse::Filepath->new(shift());
    print "> getattr " . $file->orig . "(" . $file->key . ")\n"  if($params{trace} > 10);

    #XXX use head here
    my $node = RiakFuse::Data->get($file);
    return $node unless ref $node;
    my $stat;
    if($file->key eq '%2F') {
	#we are root so our metadata is self contained
	$stat = $node->{content}->{'.'};
    } else {
	my $parent = RiakFuse::Data->get($file->parent);
	$stat = $parent->{content}->{$file->name};
    }

    return (
	0,
	0,
	$stat->{mode} + ($stat->{type} <<9),
	2,
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
