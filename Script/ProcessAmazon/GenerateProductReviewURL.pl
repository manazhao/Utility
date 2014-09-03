#!/usr/bin/perl
#
use strict;
use warnings;
use JSON;
my %item_map = ();

while(<>){
	chomp;
	my $json = decode_json($_);
	my $items = $json->{"items"};
	foreach my $item (@$items){
		my $asin = $item->{"asin"};
		next if exists $item_map{$asin};
		if(exists $item->{"rc"} and not $item->{rc} eq ""){
			my $rvwCnt = $item->{rc};
			$rvwCnt =~ s/\,//g;
			my $num_pages = int($rvwCnt/10);
			if($rvwCnt % 10){
				$num_pages++;
			}
			$item_map{$asin} = $num_pages;
		}
	}
}


while(my($asin,$num_pages) = each %item_map){
	print join(",", ($asin, $num_pages)) . "\n";
}

