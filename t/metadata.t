#!/usr/bin/perl

use strict;
use warnings;



use Test::More;

our $server = "127.0.0.1:8091";
our $filebucket = "md-test";
our $mdbucket   = "md-testmd";

require_ok("RiakFuse::MetaData::Directory");
require_ok("RiakFuse::MKFS");


RiakFuse::MKFS->clean($server, $filebucket, $mdbucket);

RiakFuse::MKFS->make($server, $filebucket, $mdbucket);

my $conf = RiakFuse::Conf->new(
    server => $server,
    filebucket => $filebucket,
    mdbucket => $mdbucket,
    mountpoint => "fake",
    );




done_testing();



