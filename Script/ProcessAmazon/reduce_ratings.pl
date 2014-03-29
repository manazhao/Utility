#!/usr/bin/perl
use warnings;
use strict;
use LWP::Simple;
use Data::Dumper;
use JSON qw(decode_json);

my $book_file = "./book.txt";
my $result_rating_file = "./processed/book_rating_san.tsv";
my $item_profile_file = "./item_profile";

# since some items in book.txt do not have profiles in item_profile
#
my %item_map = ();

open  PROFILE_FILE, $item_profile_file or die $!;
open RATING_FILE, $book_file or die $!;
open RESULT_RATING_FILE, ">",$result_rating_file or die $!;

while(<PROFILE_FILE>){
	chomp;
	my $item_json = decode_json($_);	
	my $item_id = $item_json->{"id"};
	$item_map{$item_id} = 1;
}

close PROFILE_FILE;

while(<RATING_FILE>){
	my $line = $_;
	chomp;
	my @fields = split /\s+/;
	my $item_id = $fields[1];
	if(exists $item_map{$item_id}){
		print RESULT_RATING_FILE $line;
	}
}

close RATING_FILE;
close RESULT_RATING_FILE;

