#!/usr/bin/perl
use strict;
use warnings;
use Sys::Hostname;
use Getopt::Long;
use File::Basename;

my $url_file;

my $wait = 2;
GetOptions("url-file=s" => \$url_file, "wait=s" => \$wait) or die $!;

$url_file or usage();
-f $url_file or die $!;

my $result_dir = $url_file . "_pages";
-d $result_dir or mkdir $result_dir;
# change the working directory
chdir $result_dir;

my $log_file = "./wget.log";
my $wget_cmd = "wget  -i $url_file  -o $log_file -t 3 -w $wait -nc -U 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.6) Gecko/20070802 SeaMonkey/1.1.4' ";
`$wget_cmd`;

sub usage{
	print <<END;
Usage: $0 
	--url-file	url file
	--wait		number of seconds to wait between two requests
END
die $!;
}

