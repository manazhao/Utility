#!/usr/bin/perl
use warnings;
use strict;
use LWP::Simple;
use Data::Dumper;
use JSON qw(decode_json);

my $book_file = "./processed/book_rating_san.tsv";
my $book_remap_file = "./processed/book_rating_san_remap.tsv";
my $user_mapping_file = "./processed/user_id_mapping.tsv";
my $item_mapping_file = "./processed/item_id_mapping.tsv";
my $item_attr_file = "./processed/item_attr_idx.tsv";
my $item_attr_remap_file = "./processed/item_attr_idx_remap.tsv";


open RATING_FILE,"<", $book_file or die $!;
open RATING_REMAP_FILE , ">", $book_remap_file;
open UID_MAPPING_FILE, ">", $user_mapping_file or die $!;
open IID_MAPPING_FILE, ">", $item_mapping_file or die $!;
open ITEM_ATTR_FILE, "<", $item_attr_file  or die $!;
open ITEM_ATTR_REMAP_FILE, ">", $item_attr_remap_file  or die $!;

# since some items in book.txt do not have profiles in item_profile
#
my %item_id_map = ();
my %user_id_map = ();
my $user_idx = 0;
my $item_idx = 0;

while(<RATING_FILE>){
	chomp;
	my ($user_id,$item_id,$rating,$ts) = split /\s+/;
	my $tmp_user_idx = $user_id_map{$user_id};
	if(not exists $user_id_map{$user_id}){
		$user_id_map{$user_id} = $user_idx;
		$tmp_user_idx = $user_idx;
		print UID_MAPPING_FILE join("\t",($user_id,$user_idx)) . "\n";
		$user_idx++;
	}
	my $tmp_item_idx = $item_id_map{$item_id};
	if(not exists $item_id_map{$item_id}){
		$item_id_map{$item_id} = $item_idx;
		$tmp_item_idx = $item_idx;
		print IID_MAPPING_FILE join("\t",($item_id,$item_idx)) . "\n";
		$item_idx++;
	}
	# generate re-mapped rating file
	print RATING_REMAP_FILE join("\t",($tmp_user_idx,$tmp_item_idx,$rating,$ts)) . "\n";
}

while(<ITEM_ATTR_FILE>){
	chomp;
	my($item_id,$attr_id) = split /\s+/;
	my $item_remap_id = $item_id_map{$item_id};
	if(not defined $item_remap_id){
		print "item: $item_id unmapped\n";
	}
	print ITEM_ATTR_REMAP_FILE join("\t",($item_remap_id,$attr_id))."\n";
}

close RATING_FILE;
close UID_MAPPING_FILE;
close IID_MAPPING_FILE;
close RATING_REMAP_FILE;
close ITEM_ATTR_FILE;
close ITEM_ATTR_REMAP_FILE;
