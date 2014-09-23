#!/usr/bin/perl
# merge obj information from various sources (amazon profile page and OpenBR detection result)
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
	print <<EOF;
$0 file1 file2 ... fileN (note: the result will be dumped to standard output by default)

EOF
exit 1;

}
my %id_profile_map = ();
# read through each json file
foreach(@input_files){
	my $file_name = $_;
	open JSON_FILE, "<", $file_name or die $!;
	print STDERR "read $file_name\n";
	while(<JSON_FILE>){
		chomp;
		my $json_obj = decode_json($_);
		# get id and process the rest attributes
		my $obj_id = $json_obj->{"id"};
		while(my($key,$value) = each %$json_obj){
			if(defined $value){
				$id_profile_map{$obj_id}->{$key} = $value;
			}
		}
	}
	close JSON_FILE;
}


# now print the merged profile
print STDERR "write the merged object profile to standard output\n";
while(my($id,$profile) = each %id_profile_map){
	# encode as json
	my $json_str = encode_json($profile);
	print $json_str . "\n";
}

