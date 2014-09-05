#!/usr/bin/perl use Getopt::Long;
use strict;
use warnings;
use Getopt::Long;
use File::Basename;
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

-f $local_wd or `mkdir -p $local_wd`;

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
    if(not -d getFullLocalPath($sub)){
        mkdir getFullLocalPath($sub);
        foreach my $node(@cluster_node_list){
            if(not remoteFileExist($node,getFullRemotePath($sub)),1){
                my $tmpCmd = remoteCommand($node,"mkdir -p " . getFullRemotePath($sub));
                printAndExecute($tmpCmd);
            }
        }
    }
}


# use Cluster::Manager for all the following tasks
# create cluster manager object
my $cluster_manager = new Cluster::Manager(
    node_list => \@cluster_node_list,
    cluster_user => $cluster_user,
    local_wd => $local_wd,
    cluster_wd => $cluster_wd
);

# generate the item -> number of reviews file and concatenate to a single file
my $LETTER_SEQ = [qw(aa ab ac ad ae af ag ah ai aj ak al am an)];

# check the existence of item -> page_number file
my $result_map = $cluster_manager->execute_on_cluster(
    cmd_pattern => "[ -f " . LEAF_CATEGORY_DIR . "/x%s_asin_rp.csv ] && echo 1", 
    cmd_args => $LETTER_SEQ
);

my $true_cnt = 0;
map {$true_cnt += $result_map->{$_}} keys %$result_map;


if($true_cnt != scalar keys %result_map){
    print ">>> generate asin review number file\n";
    $cluster_manager->sync_cluster_execute(
        cmd_pattern => "GenerateProductReviewURL.pl < " . LEAF_CATEGORY_DIR . "%s_parsed.json > " . LEAF_CATEGORY_DIR . "/%s_asin_rp.csv",
        cmd_args => $LETTER_SEQ
    );

    # now concatenate all the result
    my $remote_path_pattern = getFullRemotePath(LEAF_CATEGORY_DIR) . "/%s_asin_rp.csv";
    my $local_tmp_path = getFullLocalPath(LEAF_CATEGORY_DIR) . "/asin_rp.tmp.csv";
    my $local_path = getFullLocalPath(LEAF_CATEGORY_DIR) . "/asin_rp.csv";
    $cluster_manager->cluster_cat(
        remote_path_pattern => $remote_path_pattern,
        remote_path_args => $LETTER_SEQ,
        local_path => $local_path
    );

    # sort and uniq
    $cluster_manager->execute_on_local("sort $local_tmp_path|uniq > $local_path");
    # now split and distribute to cluster


}




my $local_asin_file = getFullLocalPath(PRODUCT_DIR) . "/asin.csv";
%node_pid_map = ();
if(not -f $local_asin_file){
	print ">>> extract asin from category product pages\n";
	# extract on each node
	foreach my $node_idx(0 .. $#cluster_node_list){
		my $node = $cluster_node_list[$node_idx];
		my $node_file = FILE_SPLIT_PREFIX . $node_file_suffix_map[$node_idx];
		my $input_file = getFullRemotePath(LEAF_CATEGORY_DIR) . "/$node_file" . "_parsed.json";
		my $output_file = getFullRemotePath(LEAF_CATEGORY_DIR) . "/$node_file" . "_asin.csv";
		my $perl_cmd = 'perl -lne "while(/\"asin\":\"([\d\w]+)\"/g){ print \$1}" ' . $input_file . " > $output_file 2>/dev/null & echo \$!";
		my $remote_cmd = remoteCommand($node, $perl_cmd);
		my $pid = printAndExecute($remote_cmd);
		if($pid){
			$node_pid_map{$node} = $pid;
			print ">>> extract asin on $node : $pid\n";
		}else{
			print ">>> [err] failed to extract asin on $node\n";
		}
	}
	print ">>> wait for all extraction job done\n";
	waitClusterFinish(\%node_pid_map);

	print ">>> concatenate asins of each node to a single file\n";
	# concatenate all asins 
	my $local_asin_tmp_file = getFullLocalPath(PRODUCT_DIR) . "/asin.tmp.csv";
	foreach my $node_idx(0 .. $#cluster_node_list){
		my $node = $cluster_node_list[$node_idx];
		my $node_file = FILE_SPLIT_PREFIX . $node_file_suffix_map[$node_idx];
		my $node_asin_file = getFullRemotePath(LEAF_CATEGORY_DIR) . "/$node_file" . "_asin.csv";
		# do the dump
		$cmd = "ssh $cluster_user\@$node 'cat $node_asin_file' >> $local_asin_tmp_file";
		print ">>> $cmd\n";
		`$cmd`;
	}	
	`sort $local_asin_tmp_file |uniq > $local_asin_file`;
	`rm $local_asin_tmp_file`;
# now copy to the $main_node
	$cmd = "scp $local_asin_file $cluster_user\@$main_node:" . getFullRemotePath(PRODUCT_DIR) . "/";
	print ">>> $cmd\n";
	`$cmd`;
}

# download product profile by running amazon product api
my $remote_asin_file = getFullRemotePath(PRODUCT_DIR) . "/asin.csv";

if(not remoteFileExist($main_node,$remote_asin_file)){
	print ">>> [err] asin file does not exist: $remote_asin_file\n";
	exit 1;
}

my $response_offset_file = getFullRemotePath($main_node,PRODUCT_DIR) . "/response_pos.csv";
my $response_linked_file = getFullRemotePath($main_node,PRODUCT_DIR) . "/response_linked.csv";
my $item_file = getFullRemotePath(PRODUCT_DIR) . "/profile.json";
my $log_file = getFullRemotePath(PRODUCT_DIR) . "/nohup_api.log";


my $query_item_pid = checkRemoteProcess($main_node, "crawler_console queryItem");
if(!$query_item_pid){
	$cmd = remoteCommand($main_node,"nohup crawler_console queryItem --asin-file=$remote_asin_file --offset-file=$response_linked_file --item-file=$item_file 1>$log_file 2>&1 & echo \$!");
        $query_item_pid = printAndExecute($cmd);
	if(!$query_item_pid){
		print ">>> failed to run amazon item profile query on $main_node\n";
		exit 1;
	}
}else{
	print ">>> [warn] amazon item query is running on $node: $query_item_pid";
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
