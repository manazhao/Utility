#!/usr/bin/perl
#
# remove duplciate rating and review
#
use strict;
use warnings;

my %id_map = ();
while(<>){
	my $line = $_;
	chomp;
	my($uid,$iid,$date,@rest) = split /\,/;
	my $key = join("_",($uid,$iid,$date));
	print $line unless exists $id_map{$key};
	$id_map{$key} = 1;
}
