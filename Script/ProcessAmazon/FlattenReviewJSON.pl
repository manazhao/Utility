#!/usr/bin/perl
#
# flattern parsed review json file into the following files,
# 1) user,item,rating,date
# 2) user,item.review,date
#
#
use strict;
use warnings;
use File::Basename;
use Getopt::Long;
use JSON;
use Time::Piece;
use Try::Tiny;

my $json_file;
my $rate_file;
my $review_file;
my %review_map = ();

GetOptions("json=s" => \$json_file, "rate=s" => \$rate_file, "review=s" => \$review_file) or die $!;

($json_file and $rate_file and $review_file) or die "arguments: --json=<parsed json> --rate=<output rating> --review=<output review>:$!\n";

(-f $json_file and dirname($rate_file) and dirname($review_file)) or die "file must exist:$!"; 
# read through the JSON file
#
open my $json_fh, "<" , $json_file or die $!;
open my $rate_fh, ">", $rate_file or die $!;
open my $review_fh, ">", $review_file or die $!;

print ">>> read json file\n";
while(<$json_fh>){
	chomp;
	my $json_obj = decode_json($_);
	# get item id (ASIN)
	my $pid = $json_obj->{pid};
	my $reviews = $json_obj->{r};
	next if scalar @$reviews == 0;
	foreach my $review_ref (@$reviews){
		my ($re_id,$rate,$vu,$vt,$date,$text) = get_hash_vals($review_ref,[qw(re rt vu vt d c)]);
		next if !$re_id or !$rate;
		if($date){
			# convert to timestamp
			try{
				my $t = Time::Piece->strptime($date,"%B %d, %Y");
				$date = $t->epoch;
			}catch{
				warn "illegal date format: $_ \n";
			# set $date to empty
				$date = "";
			}
		}
		my $key = join("_",($re_id,$pid,$date));
		next if exists $review_map{$key};
		$review_map{$key}=1;
		print $rate_fh join(",",($re_id,$pid,$rate,$date,$vu,$vt)) . "\n";
		next if !$text;
			# remove line change from the text
		$text =~ s/\n//g;
				# remove comma
		$text =~ s/\,//g;
			print $review_fh join(",",($re_id,$pid,$date,$text))  . "\n";
	}
}

close $json_fh;
close $rate_fh;
close $review_fh;

sub get_hash_vals{
	my($hash_ref,$keys) = @_;
	my @vals = ();
	foreach my $key(@$keys){
		push @vals, $hash_ref->{$key} ? $hash_ref->{$key} : "";
	}
	return @vals;
}
