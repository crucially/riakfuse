
use strict;
use warnings;

package RiakFuse::Filepath;
use URI::Escape;
use Data::Dumper;
sub new {
    my $class = shift;
    my $filename = shift || '/';
    my $self = bless {}, $class;
    $self->{orig} = $filename;

    

    $self->{key} = uri_escape($filename, "/");
    
    my @path = split "/", $filename;


    if (@path) {
	$self->{name} = pop @path;
	$self->{path} = join "/", @path;
    } else {
	# we are root
	$self->{name} = "";
	$self->{path} = "/";
    }
    return $self;
}

sub key {
    my $self = shift;
    return $self->{key};
}



sub path {
    my $self = shift;
    return $self->{path};
}

sub name {
    my $self = shift;
    return $self->{name};
}

sub orig {
    my $self = shift;
    return $self->{orig};
}

sub parent {
    my $self = shift;
    return bless ref($self)->new($self->{path});
}

1;
