#!/usr/bin/perl
# generate the product -> number of review pages file
# this file will be used by review downloader which expects product id and the number of pages
#

use strict;
use warnings;
use JSON;
use Getopt::Long;
use File::Basename;

# category product profile file which contains 
# basic information about products, e.g. asin (id), # of reviews
my $json_file;
my $rp_file;

GetOptions("json-file=s" => \$json_file, "rp-file=s" => \$rp_file) or die $!;
$json_file and $rp_file or usage();
-f $json_file and -d dirname($rp_file) or die "check json and rp file";

open my $json_fh, "<", $json_file or die $!;
open my $rp_fh, ">", $rp_file or die $!;

while(<$json_fh>){
	my $json_obj = decode_json($_);
	exists $json_obj->{id} or next;
	my $page = 0;
	if($json_obj->{rc}){
		my $rc_str = $json_obj->{rc};
		$rc_str =~ s/\,//g;
		$page = int($rc_str/10) + 1;
	}
	print $rp_fh $json_obj->{id} . "\t" . $page . "\n";
}
close $rp_fh;
close $json_fh;


sub usage{
	my $usage = <<EOF;
$0: [options]
	--json-file		item profile in json format
	--rp-file		output review page number file
	
EOF
	print $usage;
	exit(1);
}
