#!/usr/bin/perl
#
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin";
use Data::Dumper;

use Cluster::Manager;

my $mngr = new Cluster::Manager( node_list => [qw(irkm-1.soe.ucsc.edu irkm-2.soe.ucsc.edu)], cluster_user => "manazhao");
$mngr->init_cluster_wd("/tmp/test_cluster");
$mngr->init_local_wd("/tmp/test_cluster");

=head1 test split file and distribute to cluster
=cut
# now test split and distribute
my $local_file = "/tmp/test_cluster/Test.pl";
$mngr->split_and_distribute($local_file, "/tmp/test_cluster/", split_prefix => 'x');

# rsync local files to remote 


# cat all splits on node to local
$mngr->cluster_cat(
	remote_path_pattern => "/tmp/test_cluster/Test.pl_%02d",
	remote_path_args => [0 .. 1], 
	local_path => "/tmp/test_cluster/cat_from_node");

# test sync mult-process over cluster
#$mngr->sync_cluster_execute(cmd_pattern => "sleep 1");

my $pid = $mngr->check_cluster_process("crawler_console\\s+downloadReview");
print Dumper($pid);


