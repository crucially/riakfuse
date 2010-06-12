#!/usr/bin/perl

use strict;
use warnings;

package RiakFuse::Error;


sub new {
    my $class = shift;
    my $self = bless {} , $class;
    my %param = @_;
    $self->{response} = $param{response};
    $self->{errno}    = $param{errno};
    $self->{errmsg}   = $param{errmsg};
    return $self;
}

sub is_error { 1 };


1;
