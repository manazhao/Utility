#!/usr/bin/perl
#
# process the crawled amazon author page
# 1) link the web pages into large file
# 2) run the parser

use strict;
use warnings;
use Getopt::Long;

my $wd;
my $target_name;
my $parser;

GetOptions("wd=s" => \$wd, "target" => \$target_name, "parser=s" => \$parser) or usage();

# change the work directory
chdir $wd;
# file that contains the crawled pages
my $list_file = "$target_name.list";
# find all html files and write the result to $list_file
my $page_dir = "$target_name" . "_pages";
my $gen_list_cmd = "find $page_dir -type f -not -name *.log > $list_file";
if(not -f $list_file){
	print ">>> " . $gen_list_cmd . "\n";
	`$gen_list_cmd`;
}else{
	print ">>> $list_file exists, skip\n";
}

# run the LinkFile.pl program

my $linked_file = $target_name . "_linked.txt";
my $pos_file = $target_name . "_pos.csv";

my $link_file_program = "LinkFile.pl";
my $link_log_file = $target_name . "_link.log";
my $link_file_cmd = "$link_file_program --list=$list_file --output=$linked_file --pos=$pos_file > $link_log_file 2>&1";
if(not exists $linked_file and not exists $pos_file){
	print ">>> " . $link_file_cmd . "\n";
	`$link_file_cmd`;
}else{
	print ">>> $linked_file already exists, skip\n";
}

# now run the parser
my $parser_program = "crawler_console parser ";
my $parsed_file = $target_name . "_parsed.json";
my $parse_message_file = $target_name . "_parsed.status";
my $parserName = "Amazon/AuthorParser";
my $parser_cmd = "$parser_program --page-file=$linked_file --pos-file=$pos_file --parsed-file=$parsed_file --message-file=$parse_message_file --parser-class=$parserName";
if(not exists $parsed_file){
	print ">>> " . $parser_cmd . "\n";
	`$parser_cmd`;
}else{
	print ">>> $parsed_file already exists, skip\n";
}

sub usage{
	my $usage = <<EOF;
	$0 [options]
	--wd		working directory
	--target	directory that containing the crawled pages	
	--parser	parser will be used to parse the pages

EOF
	die $usage;
}
