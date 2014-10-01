#!/usr/bin/perl
# distribute downloading task over cluster and run it parallel
#
use strict;
use warnings;
use Getopt::Long;
use File::Basename;
use Data::Dumper;
use FindBin;
use lib "$FindBin::Bin/../Lib";
use Cluster::Manager;

my @cluster_node_list = (
	"irkm-1.soe.ucsc.edu",
	"irkm-2.soe.ucsc.edu",
	"irkm-3.soe.ucsc.edu",
	"irkm-4.soe.ucsc.edu",
	"irkm-5.soe.ucsc.edu",
	"irkm-6.soe.ucsc.edu"
);

my $LETTER_SEQ = [qw(aa ab ac ad ae af ag ah ai aj ak al am an)];
my $local_wd = "/tmp/amazon/crawl";
my $remote_wd = "/home/tmp/manazhao/amazon/crawl";
my $cluster_user;
my $url_file;
my $wait = 2;

GetOptions("local-wd=s" => \$local_wd, "remote-wd=s" => \$remote_wd, "cluster-user=s" => \$cluster_user,"url-file=s" => \$url_file,"wait=i"=>\$wait) or die $!;
$local_wd and $remote_wd and $cluster_user and $url_file or usage();
-f $url_file or die "url file - $url_file does not exist";

# use Cluster::Manager for all the following tasks
my $cluster_manager = new Cluster::Manager(
	node_list => \@cluster_node_list,
	cluster_user => $cluster_user,
	local_wd => $local_wd,
	cluster_wd => $remote_wd
);

# now distribute the file on cluster
$cluster_manager->split_and_distribute($url_file,"",split_prefix=>"x");

# now run the crawler
my $log_file = "nohup_wget.log";
my $cluster_pid_map = $cluster_manager->execute_on_cluster_bg(
	cmd_pattern => "wget.pl --url-file=x%s --wait=$wait",
	cmd_args => $LETTER_SEQ,
	log_file => $log_file
);

print ">>> wait for downloading\n";
$cluster_pid_map = $cluster_manager->check_cluster_pname("wget.pl");
$cluster_manager->wait_cluster_pid($cluster_pid_map);


sub usage{
	print <<END;
$0:
	--local-wd		local working directory
	--remote-wd		remote working directory
	--cluster-user 	user to login cluster nodes (passwordless ssh access is required)
	--url-file		file containing urls to download
	--wait			number of seconds wait between two wget requests

END
	exit(1);
}
