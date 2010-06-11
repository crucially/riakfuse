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


$root->add_child($conf, $bar);
$root->add_child($conf, $baz);


$root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));

like($root->{link}, qr/baz/, "Baz is in there");
like($root->{link}, qr/bar/, "Bar is in there");


my $foo = RiakFuse::MetaData::Directory->new(
    path => RiakFuse::Filepath->new("/foo"),
    gid  => 1,
    uid  => 1,
    mode => 0755,
    );

$root->add_child($conf, $foo);

$RiakFuse::Test::MergeSleep = 0;
my $async = threads->create(sub {
    $RiakFuse::Test::MergeSleep = 2;
    $root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
    like($root->{link}, qr/baz/, "Baz is in there");
    like($root->{link}, qr/bar/, "Bar is in there");
    like($root->{link}, qr/foo/, "Foo is in there");
			    });

$root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
like($root->{link}, qr/baz/, "Baz is in there");
like($root->{link}, qr/bar/, "Bar is in there");
like($root->{link}, qr/foo/, "Foo is in there");
$async->join;


$root->remove_child($conf, $foo);

$root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
like($root->{link}, qr/baz/, "Baz is in there");
like($root->{link}, qr/bar/, "Bar is in there");
unlike($root->{link}, qr/foo/, "Foo is in Gone");


$root->add_child($conf, $foo);
$root->remove_child($conf, $foo);
$root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
like($root->{link}, qr/baz/, "Baz is in there");
like($root->{link}, qr/bar/, "Bar is in there");
unlike($root->{link}, qr/foo/, "Foo is in Gone");

$root->add_child($conf, $foo);
$root->remove_child($conf, $foo);
$root->add_child($conf, $foo);
$root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
like($root->{link}, qr/baz/, "Baz is in there");
like($root->{link}, qr/bar/, "Bar is in there");
like($root->{link}, qr/foo/, "Foo is in there");


$root->remove_child($conf, $bar);
$async = threads->create(sub {
    $RiakFuse::Test::MergeSleep = 2;
    $root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
    like($root->{link}, qr/baz/, "Baz is in there");
    unlike($root->{link}, qr/bar/, "Bar is gone");
    unlike($root->{link}, qr/foo/, "Foo is gone");
			 });
sleep 1;
$root->remove_child($conf, $foo);
$async->join;

$root->attr(
    $conf,
    uid => 1,
    gid => 2,
    mode => 0766,
    );


$root->attr(
    $conf,
    uid => 4,
    );

$root = RiakFuse::MetaData::Directory->get($conf, RiakFuse::Filepath->new("/"));
done_testing();
