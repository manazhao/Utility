#!/usr/bin/perl
use warnings;
use strict;
use LWP::Simple;
use Data::Dumper;
use JSON qw(decode_json);

my $item_profile_file = "./item_profile";
my $item_attr_file = "./processed/item_attr_idx.tsv";
my $item_attr_merge_file = "./processed/item_attr_merge.tsv";
# maps feature name to index
my $feat_map_file = "./processed/feat_map.tsv";

#
my %name_id_map = ();

open  PROFILE_FILE, $item_profile_file or die $!;
open ITEM_ATTR_FILE, ">", $item_attr_file or die $!;
open ITEM_ATTR_MERGE_FILE, ">", $item_attr_merge_file or die $!;
open FEAT_MAP_FILE, ">", $feat_map_file or die $!;

my $feat_idx = 1;

while(<PROFILE_FILE>){
	chomp;
	my $item_json = decode_json($_);	
	my $item_id = $item_json->{"id"};
	my $item_cat = $item_json->{"c"};
	if(not defined $item_cat){
		next;
	}
	# break the category string
	my @cat_strs = split /\|/, $item_cat;
	my @item_feats = ();
	my %item_feat_map = ();
	foreach(@cat_strs){
		my @sub_cats = split /\//, $_;
		# extract the category name and id which are separated by -
		foreach(@sub_cats){
			my($cat_id,$cat_name) = split /\-/;
			$cat_id = "i_c_" . $cat_id;
			if(exists $item_feat_map{$cat_id}){
				next;
			}
			$item_feat_map{$cat_id} = 1;
			# check the existence of $cat_id
			# feature type of categorical: 2
			my $item_feat_pair;
			if(not exists $name_id_map{$cat_id}){
				$item_feat_pair = [$feat_idx,2];
				$name_id_map{$cat_id} = $item_feat_pair;
				print FEAT_MAP_FILE join("\t",($feat_idx,$cat_id,2))."\n";
				$feat_idx++;
				
			}else{
				$item_feat_pair = $name_id_map{$cat_id};
			}
			push @item_feats, $item_feat_pair;
		}
	}
	# write item features to file
	my @feat_ids = ();
	foreach(@item_feats){
		my ($idx,$name,$type) = @{$_};
		push @feat_ids, join("-",($idx,1));
		#	print join("\t",($item_id,$idx)) . "\n";
		#print join("\t",($item_id,$idx)) . "\n";
		print ITEM_ATTR_FILE join("\t",($item_id,$idx)) . "\n";
	}
	#print join("\t",($item_id,join("|",@feat_ids)))."\n";
	print ITEM_ATTR_MERGE_FILE join("\t",($item_id,join("|",@feat_ids)))."\n";
}

close PROFILE_FILE;
close ITEM_ATTR_FILE;
close FEAT_MAP_FILE;
close ITEM_ATTR_MERGE_FILE;
