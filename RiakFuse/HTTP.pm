
use strict;
use warnings;

package RiakFuse::HTTP;
use LWP::UserAgent;
use HTTP::Request;
use POSIX qw(EIO ENOENT ENOSYS EEXIST EPERM O_RDONLY O_RDWR O_APPEND O_CREAT);
use JSON;
my $ua;
use HTTP::Date;
use Sys::Hostname;
use Data::Dumper;

our $id = hostname();

sub init {
    $ua = LWP::UserAgent->new();
}

sub put {
    my $class = shift;
    my $key = shift;
    my $mime = shift;
    my $obj = shift;
    if($mime eq 'application/json') {
	$obj = to_json($obj);
    }
#    print ">> PUT '$RiakFuse::params{server}/riak/$RiakFuse::params{filebucket}/$key'\n";
    my $req = HTTP::Request->new("PUT", "$RiakFuse::params{server}/riak/$RiakFuse::params{filebucket}/$key");
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
#    print ">> Fetching $method '$RiakFuse::params{server}/riak/$RiakFuse::params{filebucket}/$key'\n";
    my $req = HTTP::Request->new($method, "$RiakFuse::params{server}/riak/$RiakFuse::params{filebucket}/$key");
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
    my $req = HTTP::Request->new("DELETE", "http://127.0.0.1:8091/riak/$RiakFuse::params{filebucket}/$key");
    $req->header("X-Riak-Client-Id", $id);
    my $resp = $ua->request($req);
    if($resp->is_success) {
	return 0;
    }
    return -EIO();
}


1;
