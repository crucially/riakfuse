#!/usr/bin/perl

#!/usr/bin/perl

use strict;
use warnings;
use POSIX qw(EIO ENOENT ENOSYS EEXIST EPERM O_RDONLY O_RDWR O_APPEND O_CREAT);

use threads;

use Test::More;

our $server = "127.0.0.1:8091";
our $filebucket = "md-test";
our $mdbucket   = "md-testmd";

require_ok("RiakFuse::MetaData::Directory");
require_ok("RiakFuse::MKFS");
require_ok("RiakFuse::Conf");
require_ok("RiakFuse::Filepath");
require_ok("RiakFuse::MetaData::File");
RiakFuse::MKFS->clean($server, $filebucket, $mdbucket);

RiakFuse::MKFS->make($server, $filebucket, $mdbucket);
sleep 1;

my $conf = RiakFuse::Conf->new(
    server => $server,
    filebucket => $filebucket,
    mdbucket => $mdbucket,
    mountpoint => "fake",
    );


my $root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));


my $bar = RiakFuse::MetaData::File->new(
    path => RiakFuse::Filepath->new("/myfile"),
    gid  => 1,
    uid  => 1,
    mode => 0755,
    );


$root->add_child($conf, $bar);


$root = RiakFuse::MetaData->get($conf, RiakFuse::Filepath->new);

my $file = RiakFuse::MetaData->get($conf, RiakFuse::Filepath->new("/myfile"));
use Data::Dumper;

$root->add_child($conf, RiakFuse::MetaData::Directory->new(
		     path => RiakFuse::Filepath->new("/baz"),
		     gid  => 1,
		     uid  => 1,
		     mode => 0755,
		 ));

my $baz = RiakFuse::MetaData->get($conf, RiakFuse::Filepath->new("/baz"));


$baz->add_child($conf, 
		RiakFuse::MetaData::File->new(
		    path => RiakFuse::Filepath->new("/baz/blah"),
		    gid  => 1,
		    uid  => 1,
		    mode => 0755,
		)
    );

$baz = RiakFuse::MetaData->get($conf, RiakFuse::Filepath->new("/baz"));


is_deeply($baz->children, ['blah']);

my $notexist = RiakFuse::MetaData->get($conf, RiakFuse::Filepath->new("/baz/dsf"));

is($notexist->{errno}, -ENOENT());


done_testing();
