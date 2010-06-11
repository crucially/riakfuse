#!/usr/bin/perl

package RiakFuse::MetaData;
use strict;
use warnings;



our %headers = (
    'X-Riak-Meta-RFS-ctime'  => 'ctime',
    'X-Riak-Meta-RFS-uid'    => 'uid',
    'X-Riak-Meta-RFS-gid'    => 'gid',
    'X-Riak-Meta-RFS-mode'   => 'mode',
    'X-Riak-Meta-RFS-type'   => 'type',
    'X-Riak-Meta-RFS-key'    => 'key',
    'X-Riak-Meta-RFS-name'   => 'name',
    'X-Riak-Meta-RFS-parent'   => 'parent',
    );

our %headers_r;

foreach my $header (keys %headers) {
    $headers_r{$headers{$header}} = $header;
}

1;


