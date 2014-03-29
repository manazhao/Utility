#!/usr/bin/perl
# decode author profile in json format and download the profile photo using wget
# the urls will be distributed to the irkm nodes and the download will take place concurrently

use strict;
use warnings;
use JSON qw(decode_json)
use Data::Dumper;

# parse the json files on irkm servers

# assume the script is shipped to the node
# first read the author json file which is located under

# note: all nodes have the identical file path
my $author_file = "/home/tmp/manazhao/data/amazon_user/author_profile/author.json";
my $author_photo_file = "/home/tmp/manazhao/data/amazon_user/author_photo/photo_url";
open AUTHOR_FILE, "<", $author_file or die $!;
open AUTHOR_PHOTO_FILE, ">", $author_photo_file or die $!;

while(<AUTHOR_FILE>){
	chomp;
	my $json_obj = decode_json($_);
	my $photo_url = $json_obj->{"img"};
	if(defined $photo_url){
		print AUTHOR_PHOTO_FILE $photo_url . "\n";
	}
}

close AUTHOR_PHOTO_FILE;
close AUTHOR_FILE;
