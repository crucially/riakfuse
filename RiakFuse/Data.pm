
use strict;
use warnings;
package RiakFuse::Data;
use RiakFuse::HTTP;

my %on_disk;


sub fetch {
    my $class = shift;
    my $file = shift;
    my $method = shift;

    my $obj;
    if(my $cached = $on_disk{$file->key}) {
	$obj = RiakFuse::HTTP->$method($file->key, $on_disk{$file->key}->{'etag'});
#	print "Conditional get worked\n" if($obj == 0);
	return $cached if($obj == 0);
	return $obj unless ref $obj;
    } else {
	$obj = RiakFuse::HTTP->$method($file->key); 
    }
    return $obj unless (ref($obj));
    return $obj if ($method eq 'head');


    

    $on_disk{$file->key} = $obj;
    
    return $obj;

}


sub head {
    my $class = shift;
    my $file = shift;
    $class->fetch($file, 'head');
}

sub get {
    my $class = shift;
    my $file = shift;
    $class->fetch($file, 'get');
}

sub put {
    my $class = shift;
    my $file  = shift;
    my $mime  = shift;
    my $obj   = shift;
    delete ($on_disk{$file->key});
    return RiakFuse::HTTP->put($file->key, $mime,  $obj);
}

sub delete {
    my $class = shift;
    my $file  = shift;
    delete($on_disk{$file->key});
    return RiakFuse::HTTP->delete($file->key);
}
1;
