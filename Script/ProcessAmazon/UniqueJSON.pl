#!/usr/bin/perl
#
# remove dulicate json object
# each json object is identified by "id" field
use strict;
use warnings;
use JSON;
my %id_map = ();

while(<>){
	chomp;
	my $line = $_;
	my $json_obj = decode_json($line);
	not exists $id_map{$json_obj->{id}} or next;
	$id_map{$json_obj->{id}} = 1;
	print $line . "\n";	
}


