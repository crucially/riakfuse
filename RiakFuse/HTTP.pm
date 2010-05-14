
use strict;
use warnings;
use Carp;
package RiakFuse::HTTP;
use LWP::UserAgent;
use HTTP::Request;
use POSIX qw(EIO ENOENT ENOSYS EEXIST EPERM O_RDONLY O_RDWR O_APPEND O_CREAT);
use JSON;
my $ua;
use HTTP::Date;
use Sys::Hostname;
use Data::Dumper;
use Carp;

our $id = hostname();

sub CLONE {
    $ua = LWP::UserAgent->new();
}

sub timeout {
    my $class = shift;
    $ua->timeout(shift);
}

sub raw {
    my $class = shift;
    my $method = shift;
    my $url = shift;
    my $req = HTTP::Request->new($method, $url);
    return $ua->request($req);
}

sub put {
    my $class = shift;
    my $key = shift;
    my $mime = shift;
    my $obj = shift;
    confess "No mimetype\n" unless($mime);
    if($mime eq 'application/json') {
	$obj = to_json($obj);
    }
    my $server = RiakFuse->get_server;
#    print ">> PUT '$RiakFuse::params{server}/riak/$RiakFuse::params{filebucket}/$key'\n";
    my $req = HTTP::Request->new("PUT", "http://$server/riak/$RiakFuse::params{filebucket}/$key");
    $req->header("Content-Type", $mime);
    $req->header("X-Riak-Client-Id", $id);
    $req->content($obj || "");
    my $resp = $ua->request($req);
    if($resp->is_success) {
	return 0;
    } elsif ($resp->code == 404) {
	return -ENOENT();
    } else {
	return -EIO();
    }
}

sub fetch {
    my $class  = shift;
    my $key    = shift;
    my $cond   = shift;
    my $method = shift;
    my $server = RiakFuse->get_server;
#   print ">> Fetching $method 'http://$server/riak/$RiakFuse::params{filebucket}/$key'\n";

    my $req = HTTP::Request->new($method, "http://$server/riak/$RiakFuse::params{filebucket}/$key");
    $req->header("X-Riak-Client-Id", $id);
    if($cond) {
#	print ">>> Conditional get $cond\n";
	$req->header("If-None-Match", $cond);
    }
    my $resp = $ua->request($req);

    if ($cond && $resp->code == 304) {
	return 0;
    } elsif ($resp->is_success) {
	if ($method eq 'GET') {
	    if ($resp->header("Content-Type") eq 'application/json') {
		my $json = from_json($resp->content);
		return ({
		    'last-modified' => str2time($resp->header("Last-Modified")),
		    'content-length' => $resp->header("Content-Length"),
		    'content-type'   => $resp->header("Content-Type"),
		    'etag'           => $resp->header('ETag'),
		    content => $json
			});
	    } else {
		return {
		    "last-modified" => str2time($resp->header("Last-Modified")),
		    "content-length" => $resp->header("Content-Length"),
		    'content-type'   => $resp->header("Content-Type"),
		    'etag'           => $resp->header('ETag'),
		    "content" => $resp->content,
		}
	    }
	} else {
	    return {
		"last-modified"  => str2time($resp->header("Last-Modified")),
		"content-length" => $resp->header("Content-Length"),
		'content-type'   => $resp->header("Content-Type"),
		'etag'           => $resp->header('ETag',)
	    };
	}

    
    } elsif ($resp->code == 404) {
	return -ENOENT();
    } else {
	return -EIO();
    }
}

sub get {
    my $class = shift;
    my $key  = shift;
    my $cond  = shift;
    return $class->fetch($key, $cond, "GET");
}

sub head {
    my $class = shift;
    my $key  = shift;
    my $cond  = shift;
    return $class->fetch($key, $cond, "HEAD");
}

sub delete {
    my $class = shift;
    my $key = shift;
    my $server = RiakFuse->get_server;
#    print ">> DELETE http://127.0.0.1:8091/riak/$RiakFuse::params{filebucket}/$key\n";
    my $req = HTTP::Request->new("DELETE", "http://$server/riak/$RiakFuse::params{filebucket}/$key");
    $req->header("X-Riak-Client-Id", $id);
    my $resp = $ua->request($req);
    if($resp->is_success) {
	return 0;
    }
    return -EIO();
}


1;
