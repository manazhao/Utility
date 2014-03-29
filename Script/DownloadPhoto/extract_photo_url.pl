#!/usr/bin/perl
use strict;
use warnings;
# i have to put the packages under my home directory
use lib "/soe/manazhao/.cpan/build/JSON-2.90-OOELMm/lib/";
use Sys::Hostname;
use JSON qw(decode_json);
use File::Basename;


my $author_file = "/home/tmp/manazhao/data/amazon_user/author_profile/author.json";
-f $author_file or die "the author json file does not exist\n";

my $author_photo_file = "/home/tmp/manazhao/data/amazon_user/author_photo/url";
my $url_map_file = "/home/tmp/manazhao/data/amazon_user/author_photo/url_id";

my $result_dir = dirname $author_photo_file;
-d dirname $result_dir or `mkdir -p $result_dir`;

-f $author_photo_file and die("photo url file already exists, abort the extraction\n");

# first extract the photo urls
open AUTHOR_FILE, "<", $author_file or die $!;
open PHOTO_URL_FILE, ">", $author_photo_file or die $!;
open URL_MAP_FILE, ">", $url_map_file or die $!;
while(<AUTHOR_FILE>){
	chomp;
	my $json_obj = decode_json($_);
	my $author_id = $json_obj->{"id"};
	my $photo_url = $json_obj->{"img"};
	if($photo_url){
		print URL_MAP_FILE join("\t",($photo_url,$author_id)) . "\n";
		print PHOTO_URL_FILE $photo_url . "\n";
	}	
		
}

close PHOTO_URL_FILE;
close URL_MAP_FILE;
close AUTHOR_FILE;

