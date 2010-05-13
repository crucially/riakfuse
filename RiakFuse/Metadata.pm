

use strict;
use warnings;
package RiakFuse::Metadata;
use RiakFuse::HTTP;

my %cache;


sub get {
    my $class = shift;
    my $file  = shift;

    my $obj;
    my $attr;
    my $resp;
    if ($attr = $cache{$file->key}) {
	$resp = RiakFuse::HTTP->get($file->key , $attr->{"last-modified"});
	if ($resp == 0) {
	    $attr->{'not-modified'} = 1;
	    return $attr;
	}
	return $resp if ($resp < 0);
    } else {
	$resp = RiakFuse::HTTP->get($file->key);
	return $resp if ($resp < 0);
    }
    $resp->{'not-modified'} = 0;
    $cache{$file->key} = $resp;
    return $resp;
}

sub put {
    my $class = shift;
    my $file->key  = shift;
    my $obj   = shift;
    return RiakFuse::HTTP->put($file->key->key . "__fuse_metadata", "application/json",  $obj);
}


sub delete {
    my $class = shift;
    my $file->key  = shift;
    return RiakFuse::HTTP->delete($file->key->key . "__fuse_metadata");
}
1;
