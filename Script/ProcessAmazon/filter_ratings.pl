#!/usr/bin/perl #
use strict;
use warnings;
use Data::Dumper;


chdir './processed';

my $rating_file = "book_rating_san.tsv";
my $filter_rating_file = "book_rating_filter.tsv";

open RATING_FILE, "<", $rating_file or die $!;
open FILTER_RATING_FILE,">",$filter_rating_file or die $!;

# number of ratings for the user
my %user_rcnt_map = ();
my @ratings = ();

print "read rating file...\n";
while(<RATING_FILE>){
	chomp;
	my @fields = split /\t/;
	push @ratings, \@fields;
	my $userId = $fields[0];
	$user_rcnt_map{$userId}++;
}

close RATING_FILE;
# only reserver users with at least 10 ratings

print "filter out users with less than 10 ratings...\n";
my @filter_users = map { $user_rcnt_map{$_} >= 10 ? $_ : () } keys %user_rcnt_map;
my %filter_user_map = ();
@filter_user_map{@filter_users} = (1) x scalar @filter_users;

print "write the filtered users into file...\n";
foreach(@ratings){
	my $tmp_rating = $_;
	my $uid = $_->[0];
	if(exists $filter_user_map{$uid}){
		print FILTER_RATING_FILE join("\t",@{$tmp_rating}) . "\n";
	}
}
close FILTER_RATING_FILE;






