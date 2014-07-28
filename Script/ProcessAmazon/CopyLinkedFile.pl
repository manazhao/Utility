#!/usr/bin/perl
#
use strict;
use warnings;
use IO::Zlib;
use Getopt::Long;

my $list_file;
my $input_file;
my $input_position_file;
my $result_file;
my $result_position_file;

GetOptions("list=s" => \$list_file, "input-file=s" => \$input_file, "input-pos=s" => \$input_position_file, 
		   	"output-file=s" => \$result_file, "output-pos=s" => \$result_position_file) or die $!;

$list_file and $input_file and $input_position_file and $result_file and $result_position_file or die "incorrect arguments: --list=<file list> --input-file=<input> --input_pos=<input pos> --output-file=<result file> --output-pos=<output position>:$!";

my %file_pos_map = ();
# read in existing files
if(-f $input_position_file){
	open POSITION_FILE,"<", $input_position_file or die $!;
	while(<POSITION_FILE>){
		chomp;
		my ($file_name, $pos) = split /\,/;
		$file_pos_map{$file_name} = $pos;
	}
	close POSITION_FILE;
}

open my $list_fh, "<", $list_file or die $!;
my @file_list = <$list_fh>;
close $list_fh;

# remove trailing \n
map {chomp;} @file_list;
# sort by position
my @sorted_file_list = sort {$file_pos_map{$a} <=> $file_pos_map{$b}} @file_list;

print ">>> start copying...\n";
open my $output_fh, ">", $result_file or die $!;
open my $pos_fh, ">", $result_position_file or die $!;
open my $input_fh, "<", $input_file or die $!;

my %output_pos_map = ();

# go through @sorted_file_list
my $cnt = 0;
foreach my $file(@sorted_file_list){
	my $pos = $file_pos_map{$file};
	seek($input_fh,$pos,0);
	my $line = <$input_fh>;
	my $new_pos = tell $output_fh;
	$output_pos_map{$file} = $new_pos;
	print $output_fh $line;
	print $pos_fh join(",",($file,$new_pos)) . "\n";
	$cnt ++;
#	last if $cnt == 10;
}

close $output_fh;
close $pos_fh;
