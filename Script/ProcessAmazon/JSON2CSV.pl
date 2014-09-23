#!/usr/bin/perl
#
# extract fields of JSON structure and flatten as csv
#

use strict;
use warnings;
use Getopt::Long;
use JSON;
use Data::Dumper;

my $json_file;
my $field_list;
my $csv_file;
my $header = 0;

GetOptions("json=s" => \$json_file, "field=s" => \$field_list, "csv=s" => \$csv_file, "header=i" => \$header) or die $!;
$json_file and $field_list and $csv_file and defined $header or usage();

open my $json_fh, "<", $json_file or die $!;
open my $csv_fh, ">", $csv_file or die $!;

my @fields = split /\,/, $field_list;

my %fname_idx_map = ();
# number the field names
@fname_idx_map{@fields} = 0..$#fields;

$header and print $csv_fh join("\t",@fields) . "\n";

my @field_vals = ("")x@fields;
while(<$json_fh>){
	chomp;
	my $json_obj = decode_json($_);
	foreach my $field(@fields){
		$field_vals[$fname_idx_map{$field}] = defined $json_obj->{$field} ? $json_obj->{$field} : "";
	} 
	print $csv_fh join("\t",@field_vals) . "\n";
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

