#!/usr/bin/perl use Getopt::Long;
use strict;
use warnings;
use Getopt::Long;
use File::Basename;
use Data::Dumper;
use FindBin;
use lib "$FindBin::Bin/../Lib";
use Cluster::Manager;

# download the category
my $node_id;
my $node_name;
# local work directory
my $local_wd = "/tmp/amazon/crawl";
my $remote_wd = "/home/tmp/manazhao/amazon/crawl";
my $cluster_user;

# sub directory constants
use constant CATEGORY_DIR => "CategoryInfo";
use constant CATEGORY_TREE_FILE => CATEGORY_DIR . "/category_tree.csv";
use constant LEAF_CATEGORY_DIR => "LeafCategory";
use constant LEAF_CATEGORY_FILE => LEAF_CATEGORY_DIR . "/leaf_category.csv";
use constant CATEGORY_PRODUCT_URL_FILE => LEAF_CATEGORY_DIR . "/category_product_url.csv";
use constant PRODUCT_DIR => "Product";
use constant USER_DIR => "User"; use constant REVIEW_DIR => "ProductReview";
use constant LOG_FILE => "run.log";
use constant FILE_SPLIT_PREFIX => "x";


my @cluster_node_list = (
	"irkm-1.soe.ucsc.edu",
	"irkm-2.soe.ucsc.edu",
	"irkm-3.soe.ucsc.edu",
	"irkm-4.soe.ucsc.edu",
	"irkm-5.soe.ucsc.edu",
	"irkm-6.soe.ucsc.edu"
);

my @node_file_suffix_map = qw(aa ab ac ad ae af);

my $main_node = $cluster_node_list[0];

GetOptions("node-id=s" => \$node_id, "node-name=s" => \$node_name, "local-wd=s" => \$local_wd, "remote-wd=s" => \$remote_wd, "cluster-user=s" => \$cluster_user) or die $!;

$node_id  and $node_name and $local_wd and $remote_wd and $cluster_user or usage();


# get category path
$local_wd .= "/$node_name";
# update remote wd to specific product category
$remote_wd .="/$node_name";

# use Cluster::Manager for all the following tasks
my $cluster_manager = new Cluster::Manager(
    node_list => \@cluster_node_list,
    cluster_user => $cluster_user,
    local_wd => $local_wd,
    cluster_wd => $remote_wd
);

# create directory on irkm servers
my $cmd;
# create subdirectories
my @sub_dirs = (
	CATEGORY_DIR,
	LEAF_CATEGORY_DIR,
	PRODUCT_DIR,
	REVIEW_DIR,
	USER_DIR
);

# create on local machine and cluster nodes
foreach my $sub(@sub_dirs){
	if(not -d $cluster_manager->local_file_exist($sub)){
		$cluster_manager->execute_on_local("mkdir -p $sub");
		$cluster_manager->execute_on_cluster(
			cmd_pattern => "[ ! -d $sub ] && mkdir -p $sub"
		);
	}
}

my $main_node = 0;
# retrieve category tree
my $query_category_tree_pid;
if(not $cluster_manager->node_file_exist($main_node, CATEGORY_TREE_FILE)){
	$cmd =  "crawler_console retrieveAmazonCategory --start-node=$node_id --result-file=" . CATEGORY_TREE_FILE;
	$query_category_tree_pid = $cluster_manager->execute_on_node_bg(
		cmd_pattern => $cmd
	);
	if($query_category_tree_pid){
		print "[info] start to query amazon product categry  on node-$main_node: $query_category_tree_pid\n";
	}else{
		print "[err] error in query amazon product category\n";
	}
}

# now wait until finish
my $cluster_pid_map = $cluster_manager->check_cluster_process($main_node, "crawler_console\\s+retrieveAmazonCategory");
$cluster_manager->wait_cluster_execute($cluster_pid_map);

# generate leaf category file
$cluster_manager->execute_on_node($main_node,
	cmd_pattern => "[ ! -f " . LEAF_CATEGORY_FILE . " ] && " . 'perl -lne "chomp;  if(/(^[^\t]+)\t$/){print \$1}"  ' . CATEGORY_TREE_FILE . " > " . LEAF_CATEGORY_FILE,
);

# now  generate category product url
$cluster_manager->execute_on_node

# generate the item -> number of reviews file and concatenate to a single file
my $LETTER_SEQ = [qw(aa ab ac ad ae af ag ah ai aj ak al am an)];

if(not $cluster_manager->all_nodes_true($cluster_manager->execute_on_cluster( cmd_pattern => "[ -f " . LEAF_CATEGORY_DIR . "/x%s_asin_rp.csv ] && echo 1", cmd_args => $LETTER_SEQ))){
#if(1){
	print ">>> generate asin review number file\n";
	$cluster_manager->sync_cluster_execute(
		cmd_pattern => "GenerateProductReviewURL.pl < " . LEAF_CATEGORY_DIR . "/x%s_parsed.json > " . LEAF_CATEGORY_DIR . "/x%s_asin_rp.csv",
		cmd_args => [$LETTER_SEQ,$LETTER_SEQ],
		log_file => "" # avoid redirection
	);

	# now concatenate all the result
	my $remote_path_pattern = LEAF_CATEGORY_DIR . "/x%s_asin_rp.csv";
	my $local_tmp_path = REVIEW_DIR . "/asin_rp.tmp.csv";
	my $local_path = REVIEW_DIR . "/asin_rp.csv";

	$cluster_manager->cluster_cat(
		remote_path_pattern => $remote_path_pattern,
		remote_path_args => $LETTER_SEQ,
		local_path => $local_tmp_path
	);

	# sort and uniq
	$cluster_manager->execute_on_local("sort $local_tmp_path|uniq > $local_path");
	# remove the tmp file
	$cluster_manager->execute_on_local("rm $local_tmp_path");
	# now split and distribute to cluster
	$cluster_manager->split_and_distribute($local_path, REVIEW_DIR, split_prefix => 'x');
	# generate the asin file
	my $asin_file = PRODUCT_DIR . "/asin.csv";
	$cluster_manager->execute_on_local("cut -f1 -d, $local_path > $asin_file");
	# upload to the first node
	$cluster_manager->rsync_to_node(0, $asin_file, PRODUCT_DIR);
}

my $asin_file = PRODUCT_DIR . "/asin.csv";

if(not $cluster_manager->node_file_exist(0, PRODUCT_DIR . "/response_linked.csv") ){
	my $query_item_pid = $cluster_manager->check_node_process(0,"crawler_console\\s+queryItem");
	if(!$query_item_pid){
		print "[info] start to query amazon item profile on node-0\n";
		my $response_offset_file = PRODUCT_DIR . "/response_pos.csv";
		my $response_linked_file = PRODUCT_DIR . "/response_linked.csv";
		my $item_file = PRODUCT_DIR . "/profile.json";
		my $log_file = PRODUCT_DIR . "/nohup_api.log";
		my $cmd = "nohup crawler_console queryItem --asin-file=$asin_file --offset-file=$response_offset_file --response-file=$response_linked_file --item-file=$item_file";
		$query_item_pid = $cluster_manager->execute_on_node_bg(0, $cmd, log_file => $log_file);
		if($query_item_pid){
			print "[info] query item is running on node-0: $query_item_pid\n";
		}
	}else {
		print "[info] amazon item profile is running on node-0: $query_item_pid\n";
	}
}

# start to download the review pages
my $review_pid_map = {};
if(not $cluster_manager->all_nodes_true($cluster_manager->execute_on_cluster( cmd_pattern => "[ -d " . REVIEW_DIR . "/x%s_pages ] && echo 1", cmd_args => $LETTER_SEQ))){
	print "[info] start to download review pages\n";
	$review_pid_map = $cluster_manager->execute_on_cluster_bg(
		cmd_pattern => "crawler_console downloadReview --input=" . REVIEW_DIR . "/x%s --wait=2",
		cmd_args => $LETTER_SEQ
	);
}


# start to parse review pages after all pages are downloaded
$review_pid_map = $cluster_manager->check_cluster_process("crawler_console\\s+downloadReview");
print "[info]: wait for review downloading finish\n";
$cluster_manager->wait_cluster_execute($review_pid_map);

# parse the review pages
$cluster_manager->sync_cluster_execute(
	cmd_patetrn => "[  -f " . REVIEW_DIR . "/x%s_linked ] && ProcessCrawledPage.pl --wd=ProductReview --target=x%s --parser=Amazon/ProductReviewXMLParser",
	cmd_args => [$LETTER_SEQ,$LETTER_SEQ]
);

# now concatenate all parsed review files and put to irkm-1
print "[info] concatenate all review files and upload to irkm-1\n";
if($cluster_manager->all_nodes_true($cluster_manager->execute_on_cluster( cmd_pattern => "[ -f " . REVIEW_DIR . "/x%s_parsed.json ] && echo 1", cmd_args => $LETTER_SEQ))){
	my $merged_file = REVIEW_DIR . "/review.json";
	$cluster_manager->cluster_cat(
		remote_path_pattern => REVIEW_DIR . "/x%s_parsed.json",
		remote_path_args => $LETTER_SEQ,
		local_path => $merged_file
	);
	print "[info] upload review file to node-0\n";
	$cluster_manager->rsync_to_node(0,$merged_file, REVIEW_DIR . "/");
}else{
	print "[warn] parsed reviewed data is not ready\n";
}

# now generate review pages and crawl the query pages
sub waitClusterFinish{
	my($node_pid_map) = @_;
	my $num_processes = scalar keys %$node_pid_map;
	while($num_processes > 0){
		while(my($node,$pid) = each %$node_pid_map){
			if(exists $node_pid_map->{$node} and !checkRemotePid($node,$pid)){
				delete $node_pid_map->{$node};
				$num_processes--;
			}
		}
		# sleep 5 seconds between checks
		sleep(5);
		print ".";
	}
	print "\n>>> all nodes return\n";
}

sub remoteWget{
	my ($host, $input_url_file, $wait) = @_;
	# get the path
	my $dir = dirname($input_url_file);
	my $file_name = basename($input_url_file);
	my $page_dir = $input_url_file . "_pages";
	if(!remoteFileExist($host,$page_dir,1)){
		# create the directory
		my $cmd = remoteCommand($host, "mkdir $page_dir");
		printAndExecute($cmd);
	}
	my $log_file = "$dir/nohup_wget.log";
	my $wget_cmd = "wget.pl --url-file=$input_url_file --wait=$wait";
	my $full_cmd = "nohup $wget_cmd >$log_file 2>&1 & echo \$! " ;
	my $remote_cmd = remoteCommand($host, $full_cmd);
	return printAndExecute($remote_cmd);
}

sub checkRemoteProcess{
	my($host,$pname) = @_;
	my $cmd = remoteCommand($host,"ps ax|grep $pname");
	my $output = printAndExecute($cmd);
	chomp $output;
	my $pid;
	if($output){
		if($output =~ /^\s*(\d+)/){
			$pid = $1;
		}
	}
	return $pid;
}

sub checkRemotePid{
	my($host,$pid) = @_;
	my $cmd = remoteCommand($host,'ps cax|grep \s*' . $pid . ' 2>&1 ');
	my $output = `$cmd`;
	chomp $output;
	if($output =~ m/$pid/){
		return 1;
	}
	return 0;
}

sub remoteFileExist{
	my($host, $file, $isDir) = @_;
	my $test = $isDir ? "-d" : "-f";
	my $ssh_cmd = remoteCommand($host, "[ $test $file ] && echo exists");
	my $output = printAndExecute($ssh_cmd);
	return not( $output eq "");
}

# download the category page

sub remoteCommand{
	my($host,$cmd) = @_;
	return "ssh $cluster_user\@$host '$cmd'";
}


sub printAndExecute{
	my($cmd) = @_;
	print ">>> " . $cmd . "\n";
	my $cmdOutput = `$cmd`;
	chomp $cmdOutput;
	return $cmdOutput;
}

sub getFullRemotePath{
	my ($path) = @_;
	return $remote_wd . "/" . $path;
}

sub getFullLocalPath{
	my ($path) = @_;
	return $local_wd . "/" . $path;
}


sub rsyncToNode{
	my ($host,$local_dir,$remote_dir) = @_;
	my $cmd = "rsync -avrz $local_dir $cluster_user\@$host:$remote_dir";
	printAndExecute($cmd);
}

sub rsyncFromNode{
	my ($host,$remote_dir,$local_dir) = @_;
	my $cmd = "rsync -avrz $cluster_user\@$host:$remote_dir $local_dir";
	printAndExecute($cmd);
}

sub rsyncToCluster{
	my ($local_dir, $remote_dir) = @_;
	foreach my $node(@cluster_node_list){
		rsyncToNode($node,$local_dir,$remote_dir);
	}
}

sub executeCluster{
	my ($cmd,$bg) = @_;
	foreach my $node(@cluster_node_list){
		my $sshCmd = remoteCommand($node,$cmd,$bg);
		printAndExecute($sshCmd);
	}
}


sub usage{
	print <<END;
	$0:
   --node-id		category Id
   --node-name		category name
   --local-wd		local working directory
   --remote-wd		remote working directory
   --cluster-user 	user to login cluster nodes (passwordless ssh access is required)
END
	die $!;
}
