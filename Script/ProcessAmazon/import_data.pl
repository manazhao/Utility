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

# now try to import the data

my $item_profile_file = "./item_profile";
open PROFILE_FILE,"<",$item_profile_file or die $!;

while(<PROFILE_FILE>){
	chomp;
	my $item_json = decode_json($_);
	my $item_id = $item_json->{"id"};
	my $item_cat = $item_json->{"c"};
	if(not defined $item_cat){
		next;
	}
	my @cat_strs = split /\|/, $item_cat;
	my @item_feats = ();
	my %item_feat_map = ();
	foreach(@cat_strs){
		my @sub_cats = split /\//, $_;
		foreach(@sub_cats){
			# generate the feature
			my($cat_id,$cat_name) = split /\-/;
			$cat_id = "i_c_" . $cat_id;
			if(exists $item_feat_map{$cat_id}){
				next;
			}	
			$item_feat_map{$cat_id} = 1;
			push @item_feats, {"name"=>$cat_id,"type"=>"2","value"=>"1"};
		}
	}
#	print join("\t", ($item_id,join(",",@item_feats))) . "\n";
	my $entity = { "id"=>$item_id, "type"=>"3","feature"=>\@item_feats };
	my $entityJson = encode_json($entity);
	print encode_json($entity) . "\n";
	# sent it out
	$client->add_entity($entityJson);
#	last;
}

close PROFILE_FILE;
$transport->close();
