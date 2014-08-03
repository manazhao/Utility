#!/usr/bin/perl
use strict;
use warnings;
use Sys::Hostname;
use Getopt::Long;
use File::Basename;

my $url_file_prefix;

GetOptions("prefix=s" => \$url_file_prefix) or die $!;

$url_file_prefix or usage();
my %host_file_map = ("irkm-1"=>"aa","irkm-2"=>"ab","irkm-3"=>"ac","irkm-4"=>"ad","irkm-5"=>"ae","irkm-6"=>"af");
my $host_name =  hostname;
my $url_file = $url_file_prefix . $host_file_map{$host_name};
print ">>> host name:$host_name, url file: $url_file\n";
# check file existence
-f $url_file or die $!;

my $result_dir = $url_file . "_pages";
-d $result_dir or mkdir $result_dir;
# change the working directory
chdir $result_dir;

my $log_file = "./wget.log";
my $wget_cmd = "wget -b -i $url_file  -o $log_file -t 10 -w 3  -U 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.6) Gecko/20070802 SeaMonkey/1.1.4' ";
print $wget_cmd . "\n";
`$wget_cmd`;

sub usage{
	print <<END;
Usage: $0 
	--prefix	url file prefix
END
die $!;
}

