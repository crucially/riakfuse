
use strict;
use warnings;

use lib '.';
use RiakFuse;

RiakFuse::conf(
    mountpoint => "/tmp/foo",
    trace => 2,
    trace => 2,
    threaded => 0,
    bufferdir => "/tmp/buffer",
    filebucket => "filesystem1",
    logbucket => "filesystem1_log",
    server => "http://127.0.0.1:8091/",
    );




