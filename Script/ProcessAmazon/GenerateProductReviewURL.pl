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
my $cmd ="JSON2CSV.pl --json=$json_file --csv=$rp_file --field=\"id,rc\"";
print $cmd . "\n";
`$cmd`;

sub usage{
	my $usage = <<EOF;
$0: [options]
	--json-file		item profile in json format
	--rp-file		output review page number file
	
EOF
	print $usage;
	exit(1);
}
