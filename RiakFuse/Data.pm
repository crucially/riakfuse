
use strict;
use warnings;
package RiakFuse::Data;

use threads::shared;
use Data::Dumper;
use LWP::UserAgent;
use HTTP::Request;
use RiakFuse::Error;
use POSIX qw(EIO ENOENT ENOSYS EEXIST EPERM O_RDONLY O_RDWR O_APPEND O_CREAT);

my %data;
my %etag;
my %modified;
my %open;

sub open {
    my $class = shift;
    my $conf  = shift;
    my $path  = shift;

    my $self = $class->fetch($conf,$path);
    return $self if ($self->is_error);

    $open{$path->key}++;
    $data{$path->key} = $self->{content};
    $etag{$path->key} = $self->{etag};
    $modified{$path->key} = 0;

    return bless { content => \$data{$path->key}, etag => $etag{$path->key}};
}

sub read {
    my $class = shift;
    my $conf  = shift;
    my $path  = shift;
    
    if ($data{$path->key}) {
	my $open = $open{$path->key} || 0;
	if ($modified{$path->key}) {
	    # this has been modified so return our version
	    return bless { open => $open, content => \$data{$path->key}, etag => $etag{$path->key}};
	} 
	my $content = $class->fetch($conf, $path, $etag{$path->key});

	return bless { open => $open, content => \$data{$path->key}, etag => $etag{$path->key}} if(!$content);
	return $content if $content->is_error;

	#ok, so we got a different object here reset the cache
	$data{$path->key} = $content->{content};
	$etag{$path->key} = $content->{etag};
	$modified{$path->key} = 0;
    } else {

	my $content = $class->fetch($conf, $path, $etag{$path->key});
	return bless { open => 0, content => \$data{$path->key}, etag => $etag{$path->key}} if(!$content);
	return $content if $content->is_error;


    }

}


sub write {
    my $class = shift;
    my $conf  = shift;
    my $path  = shift;
   
    my $open = $open{$path->key} || 0;
    $modified{$path->key} = time;
    if (!$modified{$path->key}) {
	# we haven't modified it yet so lets get the new version
	my $content = $class->fetch($conf, $path, $etag{$path->key});
	return $content if $content->is_error;
    } 
    return bless { open => $open, content => \$data{$path->key}, etag => $etag{$path->key}};
}

sub release {
    my $class = shift;
    my $conf = shift;
    my $path = shift;
    delete $open{$path->key};
    delete $data{$path->key};
    delete $modified{$path->key};
    delete $etag{$path->key};

    return 0;

}

sub fetch {
    my $class = shift;
    my $conf  = shift;
    my $path  = shift;
    my $etag = shift || "";

    my $request = HTTP::Request->new("GET", $conf->fsurl . $path->key);

    $request->header("If-None-Match", $etag) if $etag;
    my $response = LWP::UserAgent->new->request($request);
    

    if ($response->code == 304) {
	return 0;
    }

    my $self = bless {};

    $self->{key} = $path->{key};
    $self->{content} = "";

    if ($response->code == 404) {
	#assume that nothing is there, we got the reference so return empty
	return $self;
    }
    if ($response->code == 200) {
	$self->{content} = $response->content();
	$self->{etag}    = $response->header('ETag');
	return $self;
    }

    return RiakFuse::Error->new(response => $response, errno => -EIO());    
}

sub save {
    my $class = shift;
    my $conf = shift;
    my $path = shift;

    print "Saving " . $path->key . "\n";
    my $request = HTTP::Request->new("POST", $conf->fsurl . $path->key);
    $request->header("Content-Type" => "text/plain");
    $request->content($data{$path->key});

#    print $request->as_string;
    my $response = LWP::UserAgent->new()->request($request);
#    print $response->as_string;
    RiakFuse::MetaData::File->size($conf, $path, length($data{$path->key}), 0);
}

sub is_error { 0 }
1;
