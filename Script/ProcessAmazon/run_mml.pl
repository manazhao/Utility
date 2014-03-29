#!/usr/bin/perl
#
use warnings;
use strict;

my %cmd_options = (
	# recommender method options
	"--recommender" => "GlobalAverage",
	# file ooptions
	"--training-file" => "book_rating_train.tsv",
	"--test-file" => "book_rating_test.tsv",
	"--item-attributes" => "item_attr_idx_remap.tsv",
	"--no-id-mapping" => "",
	#"--save-model" => "train.model",
	# evaluation options
	"--cross-validation" => 4
);

my $wd = "./processed";
print "switch working directory to : $wd \n";
chdir $wd;
my $rate_pred_cmd = "rating_prediction";
my $cmd_line = join(" ",($rate_pred_cmd,map {join(($cmd_options{$_} eq "" ? "" : "="),($_,$cmd_options{$_}))} keys %cmd_options));
print $cmd_line . "\n";





