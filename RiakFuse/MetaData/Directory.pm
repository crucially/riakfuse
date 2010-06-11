 
use strict;
use warnings;

package RiakFuse::MetaData::Directory;



sub new {
    my $self = shift;
    my $riakfs = shift;
    my $path = shift;
    die unless $path;
    my $self = bless {};

    $self->{path} = $path;
}





1;
