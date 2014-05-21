#!/usr/bin/perl -w
#
use strict;
use warnings;
use JSON qw(encode_json);

while(<STDIN>){
	my @fields = split /\t/;
	# construct json	
	my %json_obj = ("u" => $fields[0], "i"=> $fields[1], "r" => $fields[2]);
	my $json_str = encode_json(\%json_obj);
	print $json_str . "\n";
}
