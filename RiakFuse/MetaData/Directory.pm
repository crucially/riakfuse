 
use strict;
use warnings;

package RiakFuse::MetaData::Directory;
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Date;
use threads::shared;
use RiakFuse::MetaData;
use Time::HiRes qw(time);

$RiakFuse::Test::MergeSleep = 0;

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
    $self->{type}   = 0040;
    return $self;
}


sub get {
    my $self = shift;
    my $conf = shift;
    my $path = shift;
    $path = $path->key if (ref($path));
#    print "Fetching $path\n";
    my $request = HTTP::Request->new("GET", $conf->mdurl . $path);


    for (1..100) {
	# make sure we can get all copies in here
	$request->header("Accept", "multipart/mixed, */*;q=0.5");

	my $response = LWP::UserAgent->new->request($request);

	if($response->code == 300) {
	    # need to merge the different changes and then restart
	    $self->merge($conf, $response, $path);
	    next;
	}


	my %directory : shared;
	my $directory = \%directory;
	foreach my $header (keys %RiakFuse::MetaData::headers) {
	    $directory->{$RiakFuse::MetaData::headers{$header}} = $response->header($header);
	    
	}
	$directory->{link} = $response->header("Link");
	$directory->{mtime} = str2time($response->header("Last-Modified"));

	my $class = ref($self) || $self;
	bless $directory, $class;
	return $directory;
    }
}

sub save {
    my $self = shift;
    my $conf = shift;
    
    my $parent = $self->get($conf, $self->{parent});


}

sub add_child {
    my $parent = shift;
    my $conf   = shift;
    my $child  = shift;

    
    {
	#parent
	my $request = HTTP::Request->new("POST", $conf->mdurl . $parent->{key});
	
	$request->header("Link", "<" . "/riak/" . $conf->mdbucket . "/" .$child->{key}  . '>; riaktag="child"');
	$request->header("X-Riak-Meta-Rfs-Action" , "create");
	$request->header("Content-Type", "text/plain");
	$request->header("X-Riak-Meta-RFS-client-timestamp", time());
	
	LWP::UserAgent->new()->request($request);
    }
    {
	#child
	my $request = HTTP::Request->new("POST", $conf->mdurl . $child->{key});
	$request->header("Content-Type", "text/plain");
	foreach my $header (keys %RiakFuse::MetaData::headers_r) {
	    $request->header($RiakFuse::MetaData::headers_r{$header}, $child->{$header});
	    $request->header("X-Riak-Meta-RFS-client-timestamp", time());
	}
	LWP::UserAgent->new()->request($request);
    }

    
    
}

sub remove_child {
    my $parent = shift;
    my $conf = shift;
    my $child = shift;

    my $request = HTTP::Request->new("POST", $conf->mdurl . $parent->{key});
    
    $request->header("Link", "<" . "/riak/" . $conf->mdbucket . "/" .$child->{key}  . '>; riaktag="child"');
    $request->header("X-Riak-Meta-Rfs-Action" , "remove");
    $request->header("Content-Type", "text/plain");
    $request->header("X-Riak-Meta-RFS-client-timestamp", time());
    
    LWP::UserAgent->new()->request($request);


    my $delete = HTTP::Request->new("DELETE", $conf->mdurl . $child->{key});
    LWP::UserAgent->new()->request($delete);
}

sub attr {
    my $self = shift;
    my $conf  = shift;
    my %attr = @_;
    


    my $request = HTTP::Request->new("POST", $conf->mdurl . $self->{key});

    $request->header("Content-Type", "text/plain");

    foreach my $attr (qw (uid gid mode)) {
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

    LWP::UserAgent->new->request($request);
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
	    $request->header("X-Riak-Meta-RFS-Gid", $attr->header("X-Riak-Meta-RFS-Gid")) if($request->header("X-Riak-Meta-RFS-Gid"));
	    $request->header("X-Riak-Meta-RFS-Uid", $attr->header("X-Riak-Meta-RFS-Uid")) if($request->header("X-Riak-Meta-RFS-Uid"));
	    $request->header("X-Riak-Meta-RFS-Mode", $attr->header("X-Riak-Meta-RFS-Mode")) if($request->header("X-Riak-Meta-RFS-Mode"));
	}
    }

    my $buffer = "";

    foreach my $link (keys %links) {
	$buffer .= "<$link>; riaktag=\"child\" ,";
	if( length($buffer) > 7000) {
	    $request->push_header("Link", $buffer);
	    $buffer .= "";
	}
    }
    $request->push_header("Link", $buffer) if($buffer);
    $request->header("Content-Type", "text/plain");
#    $request->header("X-Riak-Meta-RFS-client-timestamp", time());


#    print $response->as_string;
#    print $request->as_string;
    
    LWP::UserAgent->new()->request($request);
}

1;
