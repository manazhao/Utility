#!/usr/bin/perl
#
# remove long taile products
# remove items with fewer than 5 ratings

use strict;
use warnings;

my %user_rate_map = ();

while(<>){
	my $line = $_;
	chomp;
	my ($uid,$iid,$rate,$date,$vu,$vt) = split /\,/;
	push @{$user_rate_map{$uid}}, $line;
}

while(my($uid,$lines) = each %user_rate_map){
	next if @$lines < 5;
	foreach(@$lines){
		print $_;
	}
}


