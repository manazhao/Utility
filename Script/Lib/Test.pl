#!/usr/bin/perl
#
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin";


use Cluster::Manager;


my $mngr = new Cluster::Manager( node_list => [qw(irkm-1.soe.ucsc.edu irkm-2.soe.ucsc.edu)], cluster_user => "manazhao");
$mngr->init_cluster_wd("/tmp/test_cluster");
$mngr->init_local_wd("/tmp/test_cluster");

# now test split and distribute
my $local_file = "/tmp/test_cluster/Test.pl";
$mngr->split_and_distribute($local_file, "/tmp/test_cluster/");

# cat all splits on node to local
$mngr->cluster_cat("/tmp/test_cluster/Test.pl_%02d",[0 .. 1], "/tmp/test_cluster/cat_from_node");

#

$mngr->dump();
