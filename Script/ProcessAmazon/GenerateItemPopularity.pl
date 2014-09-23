#!/usr/bin/perl
#
# generate item popularity information e.g. number of ratings and average rating
#
#
use strict;
use warnings;

my %pid_rate_map = ();

while(<>){
	chomp;
	my($uid,$pid,$rate,@rest) = split /\t/;
	$pid_rate_map{$pid}->[0]++;
	$pid_rate_map{$pid}->[1] += $rate;
}

map {$pid_rate_map{$_}->[1] /= $pid_rate_map{$_}->[0]} keys %pid_rate_map;

foreach (sort {$pid_rate_map{$b}->[0] <=> $pid_rate_map{$a}->[0]} keys %pid_rate_map){
	print join("\t",($_,@{$pid_rate_map{$_}})) . "\n";
}
