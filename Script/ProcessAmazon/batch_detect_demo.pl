#!/usr/bin/perl
#
#

use strict;
use warnings;

foreach(5..6){
	my $input_folder = "/home/qzhao2/irkm-drives/$_/data/amazon_user/author_photo/images-na.ssl-images-amazon.com/images/I";
	my $output_folder = "/home/qzhao2/irkm-drives/$_/data/amazon_user/author_photo";
	my $demo_file = $output_folder . "/author_demo.json";
	my $log_file = $output_folder. "/demo_nohup.out";
	my $cmd = "nohup perl detect_user_demo.pl --input-dir=$input_folder --output-file=$demo_file >$log_file 2>&1 &";
	print $cmd . "\n";
	`$cmd`;
}


