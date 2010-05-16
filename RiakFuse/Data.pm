
use strict;
use warnings;
package RiakFuse::Data;
use RiakFuse::HTTP;
use threads::shared;
use Data::Dumper;
my %on_disk;

my %cache;
my %inverse_cache;

sub fetch {
    my $class = shift;
    my $file = shift;
    my $method = shift;
    my $cache_ok = shift || 0;
    my $obj;

    if(my $neg = $inverse_cache{$file->key}) {
	if($neg->{stored} >= time) {
	    RiakFuse::Stats->increment("data_fetch_error_cached");
	    return $neg->{error};
	}
	delete $inverse_cache{$file->key};
    }

    my $cached = $cache{$file->key};
    if($cached &&
	(($method eq 'get' && exists $cached->{content}) || $method eq 'head')) {
	if($cache_ok && $cached->{stored} >= time) {
	    RiakFuse::Stats->increment("data_fetch_cached");
	    return $cached;
	}
	RiakFuse::Stats->increment("data_fetch_cond_attempt");
	$obj = RiakFuse::HTTP->$method($file->key, $cached->{'etag'});
	if($obj == 0) {
	    $cached->{stored} = time;
	    RiakFuse::Stats->increment("data_fetch_cond_success");
	    return $cached;
	}
    } else {
	$obj = RiakFuse::HTTP->$method($file->key); 
    }

    unless (ref($obj)) {
	$inverse_cache{$file->key} = {
	    error => $obj,
	    stored => time};
	RiakFuse::Stats->increment("data_fetch_error");
	return $obj;
    }
    
    RiakFuse::Stats->increment("data_fetch_success");

    $cache{$file->key} = $obj;
    $cache{$file->key}->{stored} = time;
    
    return $cache{$file->key};

}


sub head {
    my $class = shift;
    my $file = shift;
    my $cached_ok = shift;
    $class->fetch($file, 'head', $cached_ok);
}

sub get {
    my $class = shift;
    my $file = shift;
    my $cached_ok = shift;
    $class->fetch($file, 'get', $cached_ok);
}

sub put {
    my $class = shift;
    my $file  = shift;
    my $mime  = shift;
    my $obj   = shift;
    delete ($cache{$file->key});
    delete($inverse_cache{$file->key});
    return RiakFuse::HTTP->put($file->key, $mime,  $obj);
}

sub delete {
    my $class = shift;
    my $file  = shift;
    delete($cache{$file->key});
    delete($inverse_cache{$file->key});
    return RiakFuse::HTTP->delete($file->key);
}
1;
