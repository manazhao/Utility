#!/usr/bin/perl
use warnings;
use strict;
use LWP::Simple;
use Data::Dumper;
use JSON qw(decode_json);

my $book_file = "./processed/book_rating_san_remap.tsv";
my $train_file = "./processed/book_rating_train.tsv";
my $test_file = "./processed/book_rating_test.tsv";

open RATING_FILE,"<", $book_file or die $!;
open(my $train_fh, ">", $train_file) or die $!;
open(my $test_fh, ">", $test_file) or die $!;

my $train_percent = 0.8;

while(<RATING_FILE>){
	if(rand > $train_percent){
		print $test_fh $_;
	}else{
		print $train_fh $_;
	}
}

close RATING_FILE;
close $train_fh;
close $test_fh;
