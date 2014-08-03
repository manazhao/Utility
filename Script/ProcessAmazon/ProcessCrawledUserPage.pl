#!/usr/bin/perl
#
# process the crawled amazon author page
# 1) link the web pages into large file
# 2) run the parser

use strict;
use warnings;

my $wd = '/home/tmp/manazhao/amazon/all/AmazonToyGame';
# change the work directory
chdir $wd;
my $target_name = "author_profile";
my $list_file = "$target_name.list";
# find all html files and write the result to $list_file
my $page_dir = "*pages";
my $gen_list_cmd = "find $page_dir -type f -not -name *.log > $list_file";
print ">>> " . $gen_list_cmd . "\n";
`$gen_list_cmd`;
# run the LinkFile.pl program

my $linked_file = $target_name . "_linked.txt";
my $pos_file = $target_name . "_pos.csv";

my $link_file_program = "/soe/manazhao/tmp_home/git/Utility/Script/ProcessAmazon/LinkFile.pl";
my $link_log_file = $target_name . "_link.log";
my $link_file_cmd = "perl $link_file_program --list=$list_file --output=$linked_file --pos=$pos_file > $link_log_file 2>&1";
print ">>> " . $link_file_cmd . "\n";
`$link_file_cmd`;

# now run the parser
my $parser_program = "/soe/manazhao/tmp_home/git/SimpleCrawler/src/App/Console.php parser ";
my $parsed_file = $target_name . "_parsed.json";
my $parse_message_file = $target_name . "_parsed.log";
my $parserName = "Amazon/AuthorParser";
my $parser_cmd = "php $parser_program --page-file=$linked_file --pos-file=$pos_file --parsed-file=$parsed_file --message-file=$parse_message_file --parser-class=$parserName";
print ">>> " . $parser_cmd . "\n";
 `$parser_cmd`;
