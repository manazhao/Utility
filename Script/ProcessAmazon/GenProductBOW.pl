#!/usr/bin/perl
# generate bag of words feature
# by retrieving term vector from solr
# the product id (asin) is read from standard input

use strict;
use warnings;
use JSON;
use Data::Dumper;

while(<>){
	chomp;
	my $asin = $_;
	my $url = "\"http://localhost:8983/solr/tvrh?q=id:$asin&tv.tf=true&tv.tf_idf=true&wt=json&indent=on&fl=id,er\"";
	my $cmd = "curl $url";
	my $response = `curl $url 2>/dev/null`;
	my $json = decode_json($response);
	# notice the way of creating hash reference from array reference
	my $tv = { @{$json->{termVectors}} };
	$tv = { @{$tv->{$asin}} };
	$tv->{er} or next;
	$tv = { @{$tv->{er}} }  ;
	my $product_feature = {
		id => $asin
	};

	while(my($term,$info) = each %$tv){
		defined $info or next;
		my $info_hash = {@$info};
		$product_feature->{join("_",("tf",$term))} = int($info_hash->{tf});
		$product_feature->{join("_",("tfidf",$term))} = sprintf("%.6f",$info_hash->{"tf-idf"});
	}
	print encode_json($product_feature)."\n";
}


