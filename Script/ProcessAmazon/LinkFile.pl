#!/usr/bin/perl
#
use strict;
use warnings;
use Getopt::Long;

my $list_file;
my $result_position_file;
my $result_file;

GetOptions("list=s" => \$list_file, "output=s" => \$result_file, "pos=s" => \$result_position_file) or die $!;

($list_file and -f $list_file) or die "file list must be provided: $!";
($result_file ) or die "result file must be provided: $!";
($result_position_file) or die "result position file must be provided:$!";

my @file_list = ();

open LIST_FILE, "<", $list_file or die $!;

while(<LIST_FILE>){
	chomp;
	push @file_list,$_;
}
close LIST_FILE;

my %file_pos_map = ();
# read in existing files
if(-f $result_position_file){
	open POSITION_FILE,"<", $result_position_file or die $!;
	while(<POSITION_FILE>){
		chomp;
		my ($file_name, $pos) = split /\,/;
		$file_pos_map{$file_name} = $pos;
	}
	close POSITION_FILE;
}

# remove existing files
@file_list = grep {not exists $file_pos_map{$_} } @file_list;

open POSITION_FILE, ">>", $result_position_file or die $!;
open RESULT_FILE, ">>", $result_file or die $!;

foreach my $file(@file_list){
	my $content = `cat $file|tr "\n" ' '`;	
	# get position
	my $pos = tell RESULT_FILE;
	die "wrong file offset:$!" if $pos == -1;
	print RESULT_FILE $content . "\n";
	print POSITION_FILE join(",",($file,$pos))."\n";
}
close RESULT_FILE;
close POSITION_FILE;

# now start to write to file

