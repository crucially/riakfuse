#!/usr/bin/perl


use strict;
use warnings;
use JSON;
use LWP::UserAgent;

package RiakFuse::MKFS;
use JSON;


#XXX make it take a conf object

sub clean {
    my $self = shift;
    my $server = shift;
    my $filebucket = shift;
    my $mdbucket = shift;


    # clean data
    {
	my $resp = LWP::UserAgent->new->request(HTTP::Request->new("GET", "http://$server/riak/$filebucket?keys=yes&props=false"));
	my $keys = decode_json($resp->content);
	foreach my $key (@{$keys->{keys}}) {
	    print "delete $key\n";
	    print LWP::UserAgent->new->request(
		HTTP::Request->new("DELETE", "http://$server/riak/$filebucket/$key")
	    )->code . "\n";
	}
    }

    # clean metadata
    {
	my $resp = LWP::UserAgent->new->request(HTTP::Request->new("GET", "http://$server/riak/$mdbucket?keys=yes&props=false"));
	my $keys = decode_json($resp->content);
	foreach my $key (@{$keys->{keys}}) {
	    print "delete $key\n";
	    print LWP::UserAgent->new->request(
		HTTP::Request->new("DELETE", "http://$server/riak/$mdbucket/$key")
	    )->code . "\n";
	}
    }
}


sub make {
    my $self       = shift;
    my $server     = shift;
    my $filebucket = shift;
    my $mdbucket   = shift;

    my $req = HTTP::Request->new("POST", "http://$server/riak/$mdbucket/%2F");
    $req->header("X-Riak-Meta-RFS-ctime", time);
    $req->header("X-Riak-Meta-RFS-uid", $<);
    $req->header("X-Riak-Meta-RFS-gid", int($());
    $req->header("X-Riak-Meta-RFS-mode", 0755);
    $req->header("X-Riak-Meta-RFS-type", 0040);
    $req->header("X-Riak-Meta-RFS-key", "%2F");
    $req->header("X-Riak-Meta-RFS-name", "");
    $req->header("X-Riak-Meta-RFS-parent", "");
    $req->header("Content-Type", "text/plain");
    LWP::UserAgent->new->request($req);

    my $props = HTTP::Request->new("PUT", "http://$server/riak/$mdbucket");
    $props->header("Content-Type", "application/json");
    $props->content('{"props":{"allow_mult":1}}');
    LWP::UserAgent->new->request($props);
}


1;
