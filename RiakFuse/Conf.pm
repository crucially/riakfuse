#!/usr/bin/perl

use strict;
use warnings;

package RiakFuse::Conf;

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    my %params = @_;

    die "No server defined" if (!$params{server} && !$params{servers});
    my $servers = $params{servers} || {};
    $servers->{$params{server}} = 1 if $params{server};

    $self->{mountopt}   = $params{mountopt} || "";
    $self->{mountpoint} = $params{mountpoint} || die "No mountpoint";
    $self->{debug}      = $params{debug} || 0;
    $self->{threaded}   = $params{threaded} || 0;
    $self->{trace}      = $params{trace} || 0;
    $self->{filebucket} = $params{filebucket} || die "No file bucket";
    $self->{servers}    = $servers;
    $self->{mdbucket}   = $params{mdbucket} || "$self->{filebucket}_metadata";

    return $self;
    
}

sub server {
    my $self = shift;
    foreach my $server (@{$self->{servers}}) {
	return $server;
    }
}

sub mdurl {
    my $self = shift;
    return "http://" . $self->server . "/riak/$self->{mdbucket}/";
}

sub fsurl {
    my $self = shift;
    return "http://" . $self->server . "/riak/$self->{filebucket}/";
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
