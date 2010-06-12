
use strict;
use warnings;
package RiakFuse::Data;

use threads::shared;
use Data::Dumper;
use LWP::UserAgent;
use HTTP::Request;
use RiakFuse::Error;
use POSIX qw(EIO ENOENT ENOSYS EEXIST EPERM O_RDONLY O_RDWR O_APPEND O_CREAT);


sub fetch {
    my $class = shift;
    my $conf  = shift;
    my $path  = shift;


    my $request = HTTP::Request->new("GET", $conf->fsurl . $path->key);

    my $response = LWP::UserAgent->new->request($request);

    my $self = bless {};

    $self->{key} = $path->{key};
    $self->{content} = "";

    if ($response->code == 404) {
	#assume that nothing is there, we got the reference so return empty
	return $self;
    }
    if ($response->code== 200) {
	$self->{content} = $request->content();
	return $self;
    }

    return RiakFuse::Error->new(response => $response, errno => -EIO());    
}

sub save {
    my $self = shift;
    my $conf = shift;
    my $request = HTTP::Request->new("POST", $conf->fsurl . $self->{key});
    $request->header("Content-Type" => "text/plain");
    $request->content($self->{content});
    my $response = LWP::UserAgent->new()->request($request);
    print $response->as_string;
}

sub is_error { 0 }
1;
