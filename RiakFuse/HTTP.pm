
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
    $ua = LWP::UserAgent->new(keep_alive => 1);
}

sub timeout {
    my $class = shift;
    $ua->timeout(shift);
}

sub raw {
    my $class = shift;
    my $method = shift;
    my $url = shift;
    RiakFuse::Stats->increment("http_raw_$method");
    my $req = HTTP::Request->new($method, $url);
    return $ua->request($req);
}

sub put {
    my $class = shift;
    my $key = shift;
    my $obj = shift;
    confess "No mimetype\n" unless($obj->{'content-type'});
    if($obj->{'content-type'} eq 'application/json') {
	$obj->{content} = to_json($obj->{content});
    }
    RiakFuse::Stats->increment("http_put");
    my $server = RiakFuse->get_server;
    print ">> PUT 'http://$server/riak/$RiakFuse::params{filebucket}/$key'\n" if($RiakFuse::params{trace} > 15);
    my $req = HTTP::Request->new("PUT", "http://$server/riak/$RiakFuse::params{filebucket}/$key");
    $req->header("Content-Type", $obj->{'content-type'});
    $req->header("X-Riak-Vclock", $obj->{'x-riak-vclock'}) if($obj->{'x-riak-vclock'});
    $req->header("X-Riak-Client-Id", $id);
    foreach my $key (keys %$obj) {
	next unless $key =~/^x-riak-meta-/i;
	$req->header($key, $obj->{$key});
    }
    $req->content($obj->{content} || "");
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
   print ">> Fetching $method 'http://$server/riak/$RiakFuse::params{filebucket}/$key'\n" if($RiakFuse::params{trace} > 15);

    RiakFuse::Stats->increment("http_fetch_$method");
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
	my $rv = {
	    'last-modified' => str2time($resp->header("Last-Modified")),
	    'content-length' => $resp->header("Content-Length"),
	    'content-type'   => $resp->header("Content-Type"),
	    'etag'           => $resp->header('ETag'),
	};
	$rv->{'last-modified'} = str2time($resp->header('X-Riak-Meta-Last-Modified'))
	    if ($resp->header('X-Riak-Meta-Last-Modified'));


					  
	if ($method eq 'GET') {
	    if ($resp->header("Content-Type") eq 'application/json') {
		my $json = from_json($resp->content);
		$rv->{content} = $json;
		return $rv;
	    } else {
		$rv->{content} = $resp->content;
		return $rv;
	    }
	} else {
	    return $rv;
	}

    
    } elsif ($resp->code == 404) {
	return -ENOENT();
    } else {
	print $resp->code ."\n";
	print $resp->status_line . "\n";
	print $resp->as_string . "\n";
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
    print ">> DELETE http://$server/riak/$RiakFuse::params{filebucket}/$key\n" if($RiakFuse::params{trace} > 15);
    RiakFuse::Stats->increment("http_delete");
    my $req = HTTP::Request->new("DELETE", "http://$server/riak/$RiakFuse::params{filebucket}/$key");
    $req->header("X-Riak-Client-Id", $id);
    my $resp = $ua->request($req);
    if($resp->is_success) {
	return 0;
    }
    return -EIO();
}


1;
