#!/usr/bin/perl
# merge author information from various sources (amazon profile page and OpenBR detection result)
# the files are provided from the command line arguments

use strict;
use warnings;
use JSON qw(encode_json);
use JSON qw(decode_json);

my @input_files = ();
while(my $file = shift){
	-f $file or die("input file $file does not exist");
	push @input_files, ($file);
}

if($#input_files <= 0){
	print qq{
$0 file1 file2 ... fileN
Note: the result will be dumped to standard output by default

};
}

my %id_profile_map = ();

# read through each json file
foreach(@input_files){
	my $file_name = $_;
	open JSON_FILE, "<", $file_name or die $!;
	while(<JSON_FILE>){
		chomp;
		my $json_obj = decode_json($_);
		# get id and process the rest attributes
		my $author_id = $json_obj->{"id"};
		while(my($key,$value) = each %$json_obj){
			if(defined $value){
				$id_profile_map{$author_id}->{$key} = $value;
			}
		}
	}
	close JSON_FILE;
}


# now print the merged profile

while(my($id,$profile) = each %id_profile_map){
	# encode as json
	my $json_str = encode_json($profile);
	print $json_str . "\n";
}

