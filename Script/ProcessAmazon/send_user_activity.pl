#!/usr/bin/perl
#
use strict;
use warnings;
use LWP::Simple;
use Data::Dumper;
use JSON qw(decode_json);
use JSON qw(encode_json);

use lib '/home/qzhao2/Downloads/thrift/lib/perl/lib';
use lib '/home/qzhao2/git/projects/MFVB/src/recsys/thrift/gen-perl';

use Thrift;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::BufferedTransport;

use recsys::Types;
use recsys::HandleData;


my $socket    = new Thrift::Socket('localhost',9090);
my $transport = new Thrift::BufferedTransport($socket,1024,1024);
my $protocol  = new Thrift::BinaryProtocol($transport);
my $client    = new recsys::HandleDataClient($protocol);

# open the connection
$transport->open();

# simulate some user activities and send to server

my $activity = {"uid"=>"gbl_1", "type"=>"0", "context"=>""};
my $activityJson = encode_json($activity);
#print $activityJson . "\n";
#$client->add_activity($activityJson);

my $rec_items_json = $client->get_recommend_list("gbl_1");

my $rec_items = decode_json($rec_items_json);

foreach(@$rec_items){
	print $_->{"id"} . "\t". $_->{"t"} . "\n";
}
#print Dumper $rec_items;


