#!/usr/bin/perl
#
# reverse category tree so that each node corresponds to the path to the 
# root node
#
use strict;
use warnings;
use Getopt::Long;
use File::Basename;

my $input_file;
my $output_file;

GetOptions("input=s" => \$input_file, "output=s" => \$output_file) or die $!;
$input_file and $output_file or usage();
-f $input_file or die "input file does not exist";
-d dirname($output_file) or die "output folder does not exist";

open INPUT_FILE, "<", $input_file or die $!;
open OUTPUT_FILE, ">", $output_file or die $!;

my %node_parent_map = ();
my %node_name_map = ();
my %leaf_node_map = ();

while(<INPUT_FILE>){
	chomp;
	my ($cat_str,@p_cats) = split /\t/;
	my ($pid, $pname) = split /\|/, $cat_str;
	$node_name_map{$pid} = $pname;
	if(scalar @p_cats){
		foreach(@p_cats){
			my($cid,$cname) = split /\|/;
			$node_parent_map{$cid} = $pid;
		}
	}else{
		$leaf_node_map{$pid} = 1;
	}
}

close INPUT_FILE;
foreach my $cid (keys %node_name_map){
	# identify all parent nodes
	my @parent_nodes = ();
	my $lcatName = $node_name_map{$cid};
	my $cur_node = $cid;
	$cid =~ m/^\d+$/ or print $cid . "\n";
	while(exists $node_parent_map{$cur_node}){
		$cur_node = $node_parent_map{$cur_node};
		unshift(@parent_nodes, $cur_node);
	}
	my $is_leaf = exists $leaf_node_map{$cid} ? 1 : 0;
	print OUTPUT_FILE join("\t",($cid,$lcatName,$is_leaf, "/".join("/",@parent_nodes))) . "\n";
}

close OUTPUT_FILE;



sub usage{
	print <<EOF;
	$0:	reverse category tree
	--input		category tree by Amazon product API
	--output	resulted reversed tree
EOF
	exit(1);
}

