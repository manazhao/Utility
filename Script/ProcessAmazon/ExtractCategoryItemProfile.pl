#!/usr/bin/perl
# extract item profile in the category product page
use strict;
use warnings;
use Getopt::Long;
use File::Basename;
use JSON;


my $input_file;
my $output_file;

GetOptions("input-file=s" => \$input_file, "output-file=s" => \$output_file) or die $!;

$input_file and $output_file or usage();
-f $input_file and -d dirname($output_file) or die "check input and output file";

open my $input_fh, "<" , $input_file or die $!;
open my $output_fh, ">", $output_file or die $!;

while(<$input_fh>){
	chomp;
	my $json_obj = decode_json($_);
	my $node_id = $json_obj->{node};
	my $items = $json_obj->{items};
	foreach my $item(@$items){
		$item->{lc} = $node_id;
		print $output_fh encode_json($item) . "\n";
	}
}

close $input_fh;
close $output_fh;

sub usage{
	my $usage = <<EOF;
perl $0	[options] 
	--input-file	input category product page file
	--output-file	resulted item profile file

EOF
	print $usage;
	exit(1);
}
