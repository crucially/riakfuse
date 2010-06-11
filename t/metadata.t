#!/usr/bin/perl

use strict;
use warnings;

use threads;

use Test::More;

our $server = "127.0.0.1:8091";
our $filebucket = "md-test";
our $mdbucket   = "md-testmd";

require_ok("RiakFuse::MetaData::Directory");
require_ok("RiakFuse::MKFS");
require_ok("RiakFuse::Conf");
require_ok("RiakFuse::Filepath");

RiakFuse::MKFS->clean($server, $filebucket, $mdbucket);

RiakFuse::MKFS->make($server, $filebucket, $mdbucket);

my $conf = RiakFuse::Conf->new(
    server => $server,
    filebucket => $filebucket,
    mdbucket => $mdbucket,
    mountpoint => "fake",
    );



my $root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));

use Data::Dumper;

print Dumper($root);
my $bar = RiakFuse::MetaData::Directory->new(
    path => RiakFuse::Filepath->new("/bar"),
    gid  => 1,
    uid  => 1,
    mode => 0755,
    );

my $baz = RiakFuse::MetaData::Directory->new(
    path => RiakFuse::Filepath->new("/baz"),
    gid  => 1,
    uid  => 1,
    mode => 0755,
    );
done_testing();

$root->add($conf, $bar);
$root->add($conf, $baz);


$root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));


my $foo = RiakFuse::MetaData::Directory->new(
    path => RiakFuse::Filepath->new("/foo"),
    gid  => 1,
    uid  => 1,
    mode => 0755,
    );

$root->add($conf, $foo);

my $async = threads->create(sub {
    $RiakFuse::Test::MergeSleep = 5;
    $root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
			    });

$root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
$async->join;
