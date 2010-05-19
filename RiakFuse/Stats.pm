
package RiakFuse::Stats;

use POSIX qw(ENOENT);

use strict;
use warnings;
use Fuse qw(fuse_get_context);
use JSON;
use threads;
use threads::shared;
use Data::Dumper;

my %files = (
    servers => \&get_servers,
    counters => \&get_counters,
    riak_nodes => \&get_riak_nodes,
    clients => \&get_clients,
    );


my %counters : shared;

sub increment {
    my $class = shift;
    my $counter = shift;
    $counters{$counter}++;
}

sub getdir {
    my $class = shift;
    my $file = shift;

    return (keys %files, "..",".");
}


my $params;
my $servers;

sub start {
    my $class = shift;
    $params = shift;
    $servers = \%RiakFuse::servers;
 

    RiakFuse::HTTP->timeout(5);

# disable this until I can get the server list from riak
#    # get initial list



    lock(%RiakFuse::servers);

    for my $server (@{$params->{servers}}) {
	    print "server is $server\n";

	{
	    my $resp = RiakFuse::HTTP->raw("GET","http://$server/riak/$RiakFuse::params{filebucket}?keys=false");
	    if($resp->is_success) {
		$RiakFuse::servers{$server} = 1;
	    } else {
		$RiakFuse::servers{$server} = 0;
	    }
	}
	threads->new(sub {
	    RiakFuse::HTTP->timeout(5);
	    while(1) {
		sleep 1;
		my $resp = RiakFuse::HTTP->raw("GET","http://$server/riak/$RiakFuse::params{filebucket}?keys=false");
		if($resp->is_success) {
		    $RiakFuse::servers{$server} = 1;
		} else {
		    $RiakFuse::servers{$server} = 0;
		}
	    }})->detach;
    }
    cond_signal(%RiakFuse::servers);

    
    threads->new(\&record_stats)->detach;
    



}


sub record_stats {
    while(1) {
	my $data = RiakFuse::Data->get(RiakFuse::Filepath->new("/.riakfs/clients"));
	$data = {} unless ref $data;
	$data->{content} ||= {};
	$data->{content}->{$RiakFuse::HTTP::id} = { heartbeat => time,
						   counters => get_counters()};
	$data->{'content-type'} = "application/json";
	$data->{'if-match'} = $data->{'etag'};
	my $rv = RiakFuse::Data->put(RiakFuse::Filepath->new("/.riakfs/clients"), $data);
	next if $rv == 1;
	sleep 30;
    }
}

sub get_clients {
    my ($server, $error) = RiakFuse::get_server();
    my $data = RiakFuse::Data->get(RiakFuse::Filepath->new("/.riakfs/clients"));
    my $content = "#clients";
    foreach my $client (keys %{$data->{content}}) {
	$content .= "\n\n###########################################\n";
	$content .= "client:\t$client\n";
	$content .= "heartbeat:\t" . gmtime($data->{content}->{$client}->{heartbeat}) . "\n";
	$content .= "counters:\n";
	$content .= "$data->{content}->{$client}->{counters}\n";
    }
    return $content;
}

sub get_riak_nodes {
    my ($server, $error) = RiakFuse::get_server();
    return $error if $error; 
    my $resp = RiakFuse::HTTP->raw("GET","http://$server/stats");
    if (!$resp->is_success) {
	return "Cannot reach server $server\n";
    }
    my $stats = from_json($resp->content);
    my $content = "# ring members\n";
    $content .= "$_\n" for(@{$stats->{ring_members}});
    return $content;
}



sub get_servers {
    my $content = "# server status\n";
    foreach my $server (keys %RiakFuse::servers) {
	if($RiakFuse::servers{$server}) {
	    $content .= "$server\tup\n";
	} else {
	    $content .= "$server\tdown\n";
	}
    }
    return $content;
}

sub get_counters {
    my $content;
    foreach my $key (sort keys %counters) {
	$content .= "\t$key\t$counters{$key}\n";
    }
    return $content;
}

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
	0,
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
	$resp[7] = length($files{$file->name}->());
    } else {
	return -ENOENT();
    }

    return @resp;
}


sub stats_read {
    my $class = shift;
    my $file = shift;
    my $request_size = shift;
    my $offset = shift;

    if(exists $files{$file->name}) {
	return substr($files{$file->name}->(), $offset, $request_size);
    }
    return -ENOENT();
}



sub stats_open {
    my $class = shift;
    my $file = shift;

    if(exists $files{$file->name}) {
	return 0;
    }

    return -ENOENT();
}
1;
