 
use strict;
use warnings;

package RiakFuse::MetaData::File;
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Date;
use threads::shared;
use RiakFuse::MetaData;
use Time::HiRes qw(time);

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
    $self->{type}   = $params{type}  || die "no type";
    return $self;
}



