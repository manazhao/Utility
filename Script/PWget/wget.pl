#!/usr/bin/perl
use strict;
use warnings;
use Sys::Hostname;

my %host_file_map = ("irkm-1"=>"aa","irkm-2"=>"ab","irkm-3"=>"ac","irkm-4"=>"ad","irkm-5"=>"ae","irkm-6"=>"af");

my $host_name =  hostname;
my $url_file = "url" . $host_file_map{$host_name};
print "host name:$host_name, url file: $url_file\n";

# check file existence
-f $url_file or die $!;

my $result_dir = "url-" . $host_name;
-d $result_dir or mkdir $result_dir;
`cp $url_file $result_dir/$url_file`;
chdir $result_dir;

my $log_file = "wget.log";
my $wget_cmd = "wget -i $url_file  -o $log_file -t 10 -w 3 -r -l 1 --user-agent=\"User-Agent: Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.12) Gecko/20101026 Firefox/3.6.12\"";
print $wget_cmd . "\n";
`$wget_cmd`;
