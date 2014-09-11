#!/usr/bin/perl
#
use strict;
use warnings;
use Getopt::Long;

my $large_file;
my $pos_file;
my $result_dir;

GetOptions("file=s" => \$large_file, "pos=s" => \$pos_file, "result-dir=s" => \$result_dir) or die $!;

($large_file && -f $large_file) or die "file should exist:$!";
($pos_file &&  -f $pos_file) or die "position file should exist $!";
($result_dir && -d $result_dir) or die "result directory must exist: $!";

my %pos_map = ();

open POS_FILE, "<", $pos_file or die $!;
while(<POS_FILE>){
	chomp;
	my($file_name,$pos) = split /\t/;
	$pos_map{$file_name} = $pos;
}
close POS_FILE;

open LARGE_FILE,"<", $large_file or die $!;

while(<STDIN>){
	chomp;
	my $file = $_;
	if (not exists $pos_map{$file}){
		print STDERR ">>> Warning: file not found in large file:$file\n" 
	}else{
		seek LARGE_FILE, $pos_map{$file}, 0;
		my $content = <LARGE_FILE>;
		# write the result to file
		$file =~ s/\//\|/g;
		my $output_file = join("/", ($result_dir,$file)) or die $!;
		print ">>> write to file: $output_file\n";
		open TMP_FILE,">", $output_file or die "failed to open file $output_file to write: $!";
		print TMP_FILE $content;
		close TMP_FILE;
	}
}





