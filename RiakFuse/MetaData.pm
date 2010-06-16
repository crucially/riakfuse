#!/usr/bin/perl

package RiakFuse::MetaData;
use strict;
use warnings;
use HTTP::Date;
use RiakFuse::Error;
use POSIX qw(EIO ENOENT ENOSYS EEXIST EPERM O_RDONLY O_RDWR O_APPEND O_CREAT);

$RiakFuse::Test::MergeSleep = 0;
my %OPTS = @LWP::Protocol::http::EXTRA_SOCK_OPTS;
$OPTS{MaxLineLength} = 500_000;
@LWP::Protocol::http::EXTRA_SOCK_OPTS = %OPTS;

our %headers = (
    'X-Riak-Meta-RFS-ctime'  => 'ctime',
    'X-Riak-Meta-RFS-uid'    => 'uid',
    'X-Riak-Meta-RFS-gid'    => 'gid',
    'X-Riak-Meta-RFS-mode'   => 'mode',
    'X-Riak-Meta-RFS-type'   => 'type',
    'X-Riak-Meta-RFS-key'    => 'key',
    'X-Riak-Meta-RFS-name'   => 'name',
    'X-Riak-Meta-RFS-parent'   => 'parent',
    'X-Riak-Meta-RFS-size'   => 'size',
    );

our %headers_r;

foreach my $header (keys %headers) {
    $headers_r{$headers{$header}} = $header;
}

our %cache : shared;



sub new {
    my $class = shift;
    my %params = @_;
    my %self : shared;
    my $self = bless \%self, $class;
    die unless $params{path};
    $self->{key}    = $params{path}->key;
    $self->{name}   = $params{path}->name;
    $self->{parent} = $params{path}->parent->key;
    $self->{gid}    = $params{gid}   || die "No gid";
    $self->{uid}    = $params{uid}   || die "No uid";
    $self->{mtime}  = $params{mtime} || 0;
    $self->{ctime}  = $params{ctime} || time;
    $self->{mode}   = $params{mode}  || die "no mode";
    $self->{type}   = $params{type};
    return $self;
}

sub clear_cache {
    my $self = shift;
    my $path = shift;
    delete ($cache{$path});
}

sub get {
    my $self = shift;
    my $conf = shift;
    my $path = shift;
    $path = $path->key if (ref($path));

    my $request = HTTP::Request->new("GET", $conf->mdurl . $path);

    # in same second just ignore things
    if(exists $cache{$path} && $cache{$path}->{fetch_time} == time) {
	return $cache{$path};
    }

    my $response;
    for (1..100) {
	# make sure we can get all copies in here

	$request->header("Accept", "multipart/mixed, */*;q=0.5");

	$request->header("If-None-Match", $cache{$path}->{etag}) if (exists $cache{$path});

	$response = LWP::UserAgent->new->request($request);

	if ($response->code == 304) {
	    $cache{$path}->{fetch_time} = time;
	    return $cache{$path} 
	} else {
	    delete $cache{$path};
	}

	if ($response->code == 404) {
	    # object does not exist
	    return RiakFuse::Error->new(
		response => $response,
		errno    => -ENOENT()
		);

	}

	if($response->code == 300) {
	    # need to merge the different changes and then restart
	    $self->merge($conf, $response, $path);
	    next;
	}


	my %dirent : shared;
	my $dirent = \%dirent;
	foreach my $header (keys %RiakFuse::MetaData::headers) {
	    $dirent->{$RiakFuse::MetaData::headers{$header}} = $response->header($header);
	    
	}
	$dirent->{link} = $response->header("Link");
	$dirent->{mtime} = str2time($response->header("Last-Modified"));
	$dirent->{size}  = $response->header('Content-Length') unless $dirent->{size};
	$dirent->{etag}  = $response->header('ETag');
	$dirent->{fetch_time} = time;
	
	die $response->as_string unless defined $dirent->{type};
	
	if ($dirent->{type} == 32) {
	    bless $dirent, "RiakFuse::MetaData::Directory";
	} else {
	    bless $dirent, "RiakFuse::MetaData::File";
	}
	$cache{$path} = $dirent;


	return $dirent;
    }
    return RiakFuse::Error->new(
	response => $response,
	errno    => -EIO(),
	);
}

sub is_error { 0 }

sub size {
    my $class = shift;
    my $conf = shift;
    my $path = shift;
    my $size = shift;
    my $cache_only = shift || 0;
    

    if ($cache_only && $cache{$path->key}) {
	$cache{$path->key}->{size} = $size;
	$cache{$path->key}->{size_changed}++;
    } else {
	my $request = HTTP::Request->new("POST", $conf->mdurl . $path->key);
	$request->header("Content-Type", "text/plain");
	$request->header("X-Riak-Meta-Rfs-size", $size);
	$request->header("X-Riak-Meta-RFS-client-timestamp", time());
	$request->header("X-Riak-Meta-Rfs-Action", 'attr');

	LWP::UserAgent->new->request($request);
	delete $cache{$path->key};
    }
    
    

}

sub attr {
    my $self = shift;
    my $conf  = shift;
    my %attr = @_;
    
    $self->clear_cache($self->{key});

    my $request = HTTP::Request->new("POST", $conf->mdurl . $self->{key});

    $request->header("Content-Type", "text/plain");

    foreach my $attr (qw (uid gid mode size)) {
	if (exists $attr{$attr}) {
	    my $value = delete $attr{$attr};
	    $request->header($RiakFuse::MetaData::headers_r{$attr}, $value);
	}

    }
    
    if (exists $attr{xattr}) {
	foreach my $xattr (keys %{$attr{xattr}}) {
	    if ($xattr eq 'content-type') {
		$request->header("Content-Type", $attr{xattr}->{$xattr});
		next;
	    } 
	    $request->header("X-Riak-Meta-Rfs-Xattr-$xattr", $attr{xattr}->{$xattr});
	}
    }
    $request->header("X-Riak-Meta-RFS-client-timestamp", time());
    $request->header("X-Riak-Meta-Rfs-Action", 'attr');

    my $response = LWP::UserAgent->new->request($request);

    if (!$response->is_success) {
      return RiakFuse::Error->new(
	  response => $response,
	  errno    => -EIO(),
	  );
    }
    return $self;
    
}


sub merge {
    my $class = shift;
    my $conf = shift;
    my $response = shift;
    my $key = shift;

    my $vclock = $response->header("X-Riak-Vclock");
    my @parts = $response->parts;

    my %links;
    my %links_new;
    my %links_remove;

    sleep $RiakFuse::Test::MergeSleep if ($RiakFuse::Test::MergeSleep);

    my $request = HTTP::Request->new("POST", $conf->mdurl. $key);
    $request->header("X-Riak-Vclock", $vclock);
    
    my @attr;

    foreach my $part (@parts) {
	no warnings;
	if ($part->header("X-Riak-Meta-Rfs-Action") eq 'attr') {
	    push @attr, $part;
	}
	
	foreach my $link (split "," , $part->header("Link")) {



	    my ($path) = $link =~/\<(.+)\>; riaktag=\"child\"/;
	    next unless $path;
	    my $time = $part->header("X-Riak-Meta-RFS-client-timestamp");
	    if ($part->header("X-Riak-Meta-Rfs-Action") eq 'create') {
		$links_new{$path} = $time if $time > $links_new{$path};
	    } elsif ($part->header("X-Riak-Meta-Rfs-Action") eq 'remove') {
		$links_remove{$path} = $time if $time > $links_remove{$path};
	    } else {
		$links{$path}++;
	    }
	}

	
	if ($part->header("X-Riak-Meta-Rfs-Action") eq '') {
	    # we are merging against this
	    foreach my $header (keys %RiakFuse::MetaData::headers) {
		$request->header($header, $part->header($header));
	    }	    
	}

    }

    {
	no warnings;
	foreach my $new (keys %links_new) {
	    $links{$new} = $links_new{$new};
	}

	foreach my $remove (keys %links_remove) {
	    delete $links{$remove} if ($links_remove{$remove} >  $links{$remove});
	}
    }


    
    if (@attr) {
	foreach my $attr ( sort { $a->header("X-Riak-Meta-RFS-client-timestamp") <=>  $b->header("X-Riak-Meta-RFS-client-timestamp") } @attr) {
	    $request->header("X-Riak-Meta-RFS-Gid", $attr->header("X-Riak-Meta-RFS-Gid")) if($attr->header("X-Riak-Meta-RFS-Gid"));
	    $request->header("X-Riak-Meta-RFS-Uid", $attr->header("X-Riak-Meta-RFS-Uid")) if($attr->header("X-Riak-Meta-RFS-Uid"));
	    $request->header("X-Riak-Meta-RFS-Mode", $attr->header("X-Riak-Meta-RFS-Mode")) if($attr->header("X-Riak-Meta-RFS-Mode"));
	    $request->header("X-Riak-Meta-RFS-Size", $attr->header("X-Riak-Meta-RFS-Size")) if($attr->header("X-Riak-Meta-RFS-Size"));
	}
    }

    my $buffer = "";

    foreach my $link (keys %links) {
	$buffer .= "<$link>; riaktag=\"child\" ,";
	if( length($buffer) > 3000) {
	    $request->push_header("Link", $buffer);
	    $buffer .= "";
	}
    }
    $request->push_header("Link", $buffer) if($buffer);
    $request->header("Content-Type", "text/plain");
    
    LWP::UserAgent->new()->request($request);
}


1;


