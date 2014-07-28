#!/usr/bin/perl
#
# concactenate two linked files
#
use strict;
use warnings;
use Getopt::Long;
use File::Basename;
use Fcntl qw(SEEK_SET SEEK_CUR SEEK_END);

my $file1;
my $file2;
my $pos_file1;
my $pos_file2;

# concatenate file1 to file 2
GetOptions("file1=s" => \$file1, "pos1=s" => \$pos_file1, "file2=s" => \$file2, "pos2=s" => \$pos_file2) or die $!;

$file1 and $file2 and $pos_file1, and $pos_file2 or die "--file1=<first file> --pos1=<first file position> --file2=<second file> --pos2=<second file position>:$!";
# not concatenate to itself
$file1 ne $file2 and $pos_file1 ne $pos_file2 or die "The files must be different:$!";

-f $file1 and -f $file2  and -f $pos_file1 and -f $pos_file2 or die $!;

my $file2_sz = -s $file2;

# update the byte offset of file1_pos

open my $pos1_fh, "<" ,$pos_file1 or die $!;
open my $pos2_fh, ">>", $pos_file2 or die $!;

# for each file offset in $pos_file1, increase the offset by the size of file1
print ">>> update the file offset\n";
while(<$pos1_fh>){
	my ($name,$offset) = split /\,/;
	$offset += $file2_sz;
	print $pos2_fh join(",",($name,$offset)) . "\n";
}
close $pos1_fh;
close $pos2_fh;

print ">>> concatenate two files, be patient\n";

`cat $file1 >> $file2`;

print ">>> done!\n";

