#!/usr/bin/perl
#
# get the frequence of entities
#
use strict;
use warnings;

my %line_freq_map = ();

while(<>){
	chomp;
	$line_freq_map{$_}++;
}

my @sorted_keys = sort { $line_freq_map{$b} <=> $line_freq_map{$a}} keys %line_freq_map;

foreach(@sorted_keys){
	print $_ . "," . $line_freq_map{$_} . "\n";
}
