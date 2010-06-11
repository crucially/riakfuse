#!/usr/bin/perl

use strict;
use warnings;

package RiakFuse::Conf;

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    my %params = shift;

    $self->{mountopt}   = $params{mountopt} || "";
    $self->{mountpoint} = $params{mountpoint} || die "No mountpoint";
    $self->{debug}      = $params{debug} || 0;
    $self->{threaded}   = $params{threaded} || 0;
    $self->{trace}      = $params{trace} || 0;
    $self->{filebucket} = $params{filebucket} || die "No file bucket";
    $self->{servers}    = $params{servers} || die "No servers";
    $self->{mdbucket}   = $params{mdbucket} || "$self->{filebucket}_metadata";

    return $self;
    
}








sub mountopt { return shift()->{mountopt} }
sub mountpoint { return shift()->{mountpoint} }
sub debug { return shift()->{debug} }
sub threaded { return shift()->{threaded} }
sub trace { return shift()->{trace} }
sub filebucket { return shift()->{filebucket} }
sub servers { return shift()->{servers} }
sub mdbucket { return shift()->{mdbucket} }



1;
