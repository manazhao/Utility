#!/usr/bin/perl
#
# extract fields of JSON structure and flatten as csv
#

use strict;
use warnings;
use Getopt::Long;
use JSON;

my $json_file;
my $field_list;
my $csv_file;
my $header = 0;

GetOptions("json=s" => \$json_file, "field=s" => \$field_list, "csv=s" => \$csv_file, "header=i" => \$header) or die $!;
$json_file and $field_list and $csv_file and defined $header or usage();

open my $json_fh, "<", $json_file or die $!;
open my $csv_fh, ">", $csv_file or die $!;

my @fields = split /\,/, $field_list;

$header and print $csv_fh join(",",@fields) . "\n";

while(<$json_fh>){
	chomp;
	my $json_obj = decode_json($_);
	my @field_vals = ();
	foreach my $field(@fields){
		if(exists $json_obj->{$field}){
			push @field_vals, $json_obj->{$field};
		}else{
			push @field_vals,"";
		}
	} print $csv_fh join(",",@field_vals) . "\n";
}

close $json_fh;
close $csv_fh;

sub usage{
	my $usage = <<EOF;
perl $0 [options]
	--json			input json file 
	--csv			output csv file
	--field			target fields separted by comma
	--header		whether to write header as the first line

EOF
	print $usage;
	exit(1);
}

