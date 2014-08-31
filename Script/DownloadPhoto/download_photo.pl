#!/usr/bin/perl
use strict;
use warnings;
use File::Basename;

my $author_photo_file = "/home/tmp/manazhao/data/amazon_user/author_photo/url";
-f $author_photo_file or die("photo url file does not exist\n");
my $cwd = dirname $author_photo_file;

chdir $cwd;

# download the urls
my $log_file = "wget.log";
my $wget_cmd = "wget -i $author_photo_file  -o $log_file -t 10 -w 1 -r -l 1 --user-agent=\"User-Agent: Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.12) Gecko/20101026 Firefox/3.6.12\"";
print $wget_cmd . "\n";
`$wget_cmd`;
