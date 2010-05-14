
package RiakFuse::Stats;

use POSIX qw(ENOENT);

use strict;
use warnings;
use Fuse qw(fuse_get_context);
use JSON;
use threads;
use threads::shared;
use Data::Dumper;

sub getdir {
    my $class = shift;
    my $file = shift;

    return ("servers", "..",".");
}


my $params;
my $servers;

sub start {
    my $class = shift;
    $params = shift;
    $servers = \%RiakFuse::servers;
 

# disable this until I can get the server list from riak
#    # get initial list
#    RiakFuse::HTTP->timeout(1);
#    my $resp = RiakFuse::HTTP->raw("GET","$params->{server}/stats");
#    if (!$resp->is_success) {
#	print "Cannot reach server $params->{server}\n";
#	POSIX::_exit(255);
#    }
#    my $stats = from_json($resp->content);



    print Dumper(from_json($resp->content));

}

sub get_servers {

    
}


my %files = (
    servers => 1,
    );

sub getattr {
    my $class = shift;
    my $file = shift;

    my @resp = (
	0, 
	0,
	0555 + (0040 <<9),  #mode
	2,
	fuse_get_context()->{"uid"},
	fuse_get_context()->{"gid"},
	0,
	1,
	time(),
	time(),
	time(),
	4096,
	1
	);
    if($file->orig eq '/.riakfs') {
	$resp[2] = 0555 + (0040 << 9);
    } elsif (exists $files{$file->name}) {
	$resp[2] = 0555 + (64 << 9);
    } else {
	return -ENOENT();
    }

    return @resp;
}


sub stats_read {
    my $class = shift;
    my $file = shift;

    if ($file->name eq 'servers') {
	return "";
    }
    return -ENOENT();
}



sub stats_open {
    my $class = shift;
    my $file = shift;

    if ($file->name eq 'servers') {
	return 0;
    }

    return -ENOENT();
}
1;
