 
use strict;
use warnings;

package RiakFuse::MetaData::Directory;
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Date;
use threads::shared;
use RiakFuse::MetaData;
use Time::HiRes qw(time);
use base qw(RiakFuse::MetaData);

$RiakFuse::Test::MergeSleep = 0;

sub new {
    my $self = shift;
    $self->SUPER::new(mode => 0400, @_);
}

sub children {
    my $self = shift;
    my @path;
    foreach my $link (split "," , $self->{link}) {
	my ($path) = $link =~/\<.+\%2F(.+?)\>; riaktag=\"child\"/;
	next unless $path;
	$path =~s/\+/ /g;
	$path =~s/%2B/+/g;
	push @path, $path;

    }
    return \@path;
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

    $parent->clear_cache($parent->{key});
    $parent->clear_cache($child->{key});
    
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
    $parent->clear_cache($child->{key});
    $parent->clear_cache($child->{key});
}



sub is_directory { 1 }

sub is_file { 0 }



1;
