#!/usr/bni/perl
#
# extract age, gender, race information from Facepp result
#
use strict;
use warnings;
use JSON;
use Data::Dumper;

while(<>){
	chomp;
	my $file_name = $_;
	my ($uid,@rest) = split /\_/, $file_name;
	# read the file
	open FILE, "<" , $file_name or die $!;
	my @lines = <FILE>;
	my $json_obj = decode_json(join("",@lines));
	# get age, gender and race
	foreach my $face(@$json_obj){
		my $attribute = $face->{attribute};
		my $user_obj = {};
		$user_obj->{age} = $attribute->{age}->{value}."_".$attribute->{age}->{range};
		$user_obj->{gender} = lc $attribute->{gender}->{value}."_".$attribute->{gender}->{confidence};
		$user_obj->{race} = lc $attribute->{race}->{value}."_".$attribute->{race}->{confidence};
		$user_obj->{id} = $uid;
		print encode_json($user_obj) . "\n";
		last;
	}
}
