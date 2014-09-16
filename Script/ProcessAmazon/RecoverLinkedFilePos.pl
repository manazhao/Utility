#!/usr/bin/perl
#
# position file is messed up with the amazon item query

use strict;
use warnings;
use JSON;

my $input_file = "/home/qzhao2/data/AmazonParsed/Ultimate/beauty/api_response_linked.txt";
my $pos_file = "/home/qzhao2/data/AmazonParsed/Ultimate/beauty/api_response_pos.csv";

open INPUT_FILE, "<" , $input_file or die $!;
open POS_FILE, ">", $pos_file or die $!;

my $pos = 0;
while(<INPUT_FILE>){
	chomp;
	my $json_obj = decode_json($_);
	my $asin = $json_obj->{ASIN};
	print POS_FILE join("\t",($asin,$pos)) . "\n";
	$pos = tell INPUT_FILE;
}

close POS_FILE;
close INPUT_FILE;

