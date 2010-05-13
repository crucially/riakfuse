use strict;
use warnings;

my $fsname = shift;
my $server = shift || "127.0.0.1:8091";

use LWP::UserAgent;
use JSON;

{
    my $keys = from_json(
	LWP::UserAgent->new->request(
	    HTTP::Request->new("GET", "http://$server/riak/$fsname?keys=yes&props=false")
	)->content);
    foreach my $key (@{$keys->{keys}}) {
	print "delete $key\n";
	print LWP::UserAgent->new->request(
	    HTTP::Request->new("DELETE", "http://$server/riak/$fsname/$key")
	    )->code . "\n";
    }
}
{


    my $req = HTTP::Request->new("PUT", "http://$server/riak/$fsname/%2F");
    $req->content_type("application/json");
    $req->content(to_json({
	'.' => {
	    key => '%2F',
	    ctime => time,
	    atime => time,
	    mode => 0755,
	    type => 0040,
	},
	'..' => {}
			  
			  }));

    my $resp = LWP::UserAgent->new->request($req);
    print $resp->status_line ."\n";
}
