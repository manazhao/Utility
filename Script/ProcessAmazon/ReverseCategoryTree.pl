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
	my ($cat_str,$c_nodes_str) = split /\t/;
	my ($pid, $pname) = split /\|/, $cat_str;
	$node_name_map{$pid} = $pname;
	if($c_nodes_str){
		my @splits = split /\,/, $c_nodes_str;
		foreach(@splits){
			my($cid,$cname) = split /\|/;
			$node_parent_map{$cid} = $pid;
		}
	}else{
		$leaf_node_map{$pid} = 1;
	}
}

close INPUT_FILE;
foreach my $lcat (keys %leaf_node_map){
	# identify all parent nodes
	my @parent_nodes = ();
	my $lcatName = $node_name_map{$lcat};
	my $cur_node = $lcat;
	while(exists $node_parent_map{$cur_node}){
		$cur_node = $node_parent_map{$cur_node};
		unshift(@parent_nodes, $node_name_map{$cur_node});
	}
	print OUTPUT_FILE join("\t",($lcat,$lcatName, "/".join("/",@parent_nodes))) . "\n";
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

