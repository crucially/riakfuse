 
use strict;
use warnings;

package RiakFuse::MetaData::File;
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Date;
use threads::shared;
use RiakFuse::MetaData;
use Time::HiRes qw(time);
use base qw(RiakFuse::MetaData);
sub new {
    my $self = shift;
    $self->SUPER::new(mode => 64, @_);
}

sub is_directory { 0 }

sub is_file { 1 }


1;
