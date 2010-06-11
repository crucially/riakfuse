 
use strict;
use warnings;

package RiakFuse::MetaData::Directory;
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Date;
use threads::shared;
use RiakFuse::MetaData;

sub new {
    my $class = shift;
    my %params = @_;
    my %self : shared;
    my $self = bless \%self, $class;
    die unless $params{path};
    $self->{key}    = $params{path}->key;
    $self->{name}   = $params{path}->name;
    $self->{parent} = $params{path}->parent;
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
    print "Fetching $path\n";
    my $request = HTTP::Request->new("HEAD", $conf->mdurl . $path);
    
    my $response = LWP::UserAgent->new->request($request);
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

sub save {
    my $self = shift;
    my $conf = shift;
    
    my $parent = $self->get($conf, $self->{parent});

    $parent->add($conf, $self);
    

}

sub add {
    my $parent = shift;
    my $conf   = shift;
    my $child  = shift;

    my $request = HTTP::Request->new("POST", $conf->mdurl . $parent->{key});
    
    foreach my $header (keys %RiakFuse::MetaData::headers_r) {
	$request->header($RiakFuse::MetaData::headers_r{$header}, $parent->{$header});
	
    }
    $request->header("Link", "<" . "/riak/" . $conf->mdbucket . "/" .$child->{key}  . '>; riaktag="child"');
    $request->header("Content-Type", "text/plain");
    print $request->as_string;

    print LWP::UserAgent->new()->request($request)->as_string
}
1;
