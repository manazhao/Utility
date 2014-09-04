#!/usr/bin/perl use Getopt::Long;
use strict;
use warnings;
use Getopt::Long;
use File::Basename;

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



if(not remoteFileExist($main_node,getFullRemotePath(CATEGORY_TREE_FILE))){
    $cmd = remoteCommand($main_node, "crawler_console retrieveAmazonCategory --start-node=$node_id --result-file=" . getFullRemotePath(CATEGORY_TREE_FILE));
    printAndExecute($cmd);
}else{
    print ">>> warn: category tree already exists: skip\n";
}

# generate leaf category file
if(not remoteFileExist($main_node,getFullRemotePath(LEAF_CATEGORY_FILE))){
    my $perl_cmd = 'perl -lne "chomp;  if(/(^[^\t]+)\t$/){print \$1}"  ' . getFullRemotePath(CATEGORY_TREE_FILE) . " > " . getFullRemotePath(LEAF_CATEGORY_FILE);
    $cmd = remoteCommand($main_node,$perl_cmd);
    printAndExecute($cmd);
}else{
    print ">>> warn: leaf category file already exist\n";
}

# generate category product page url
if(not remoteFileExist($main_node,getFullRemotePath(CATEGORY_PRODUCT_URL_FILE))){
    $cmd = remoteCommand($main_node, "cat " . getFullRemotePath(LEAF_CATEGORY_FILE) .  " |cut -f1 -d\\| |GenerateCategoryProductURL.pl > " . getFullRemotePath(CATEGORY_PRODUCT_URL_FILE));
    printAndExecute($cmd);
}else{
    print ">>> warn: category product url already exists: skip\n";
}

# now split the url list and distribute to other cluster nodes
if(not remoteFileExist($main_node, getFullRemotePath(CATEGORY_PRODUCT_URL_FILE))){
    print ">>> warn: category product url file: " . getFullRemotePath(CATEGORY_PRODUCT_URL_FILE) . " does not exist, abort now! \n";
    exit(1);
}


# if split not exist yet
my $category_product_url_split = getFullLocalPath(LEAF_CATEGORY_DIR) . "/" . FILE_SPLIT_PREFIX . $node_file_suffix_map[0];
print $category_product_url_split  ."\n";

# split the url and distribute to nodes
if(not -f $category_product_url_split){
# we need to copy to local working directory and split it
    rsyncFromNode($main_node, getFullRemotePath(LEAF_CATEGORY_DIR), "$local_wd/");
# count lines
    $cmd = "wc -l " . getFullLocalPath(CATEGORY_PRODUCT_URL_FILE);
    my $num_of_urls = printAndExecute($cmd);
    if($num_of_urls){
        if($num_of_urls =~ m/^(\d+)/){
            $num_of_urls = $1;
        }
    }
    print ">>> number of category product url: $num_of_urls \n";
# split
    my $split_size = int($num_of_urls/6) + 1;
    $cmd = "cd " . getFullLocalPath(LEAF_CATEGORY_DIR) . ";" .  "split -l  $split_size " . getFullLocalPath(CATEGORY_PRODUCT_URL_FILE) . " " . FILE_SPLIT_PREFIX;
    printAndExecute($cmd);

# now distribute to all nodes
    rsyncToCluster(getFullLocalPath(LEAF_CATEGORY_DIR), "$remote_wd/");
}else{
    print ">>> warn: category product url already splitted\n";
}


# running pwget if the pages are not downloaded yet
my %node_pid_map = ();

foreach my $node_idx (0 .. $#cluster_node_list){
    my $node = $cluster_node_list[$node_idx];
    my $file_suffix = $node_file_suffix_map[$node_idx];
    my $node_file = getFullRemotePath(LEAF_CATEGORY_DIR) . "/" . FILE_SPLIT_PREFIX . $file_suffix;
    my $result_dir = $node_file . '_pages';
    if(not remoteFileExist($node,$result_dir,1)){
        my $pid = remoteWget($node,$node_file,3);
        if(!$pid){
            print ">> [err] $node: wget failed\n";
        }else{
            print ">>> wget on $node: $pid\n";
	    $node_pid_map{$node} = $pid;
    }
    }else{
	    my $pid = checkRemoteProcess($node,"wget.pl");
	    if($pid){
		    print ">>> downloading is still running: $pid\n";
		    $node_pid_map{$node} = $pid;
	    }
    }
}

# wait for all category product page download to finish
if(scalar keys %node_pid_map){
	print ">>> wait for all category product page download to finish\n";
	waitClusterFinish(\%node_pid_map);
}

# process the downloaded category product pages
%node_pid_map = ();
foreach my $node_idx( 0 .. $#cluster_node_list){
	my $node = $cluster_node_list[$node_idx];
	my $node_wd = getFullRemotePath(LEAF_CATEGORY_DIR);
	my $node_target = FILE_SPLIT_PREFIX . $node_file_suffix_map[$node_idx];
	my $log = getFullRemotePath(LEAF_CATEGORY_DIR) . "/nohup_process_crawl.log";
	my $remote_cmd = remoteCommand($node, "nohup ProcessCrawledPage.pl --wd=$node_wd --target=$node_target --parser=Amazon/CategoryPageParser 1>$log 2>&1 & echo \$!" );
	my $pid = printAndExecute($remote_cmd);
	if($pid){
		print ">>> ProcessCrawledPage.pl on $node: $pid\n";
		$node_pid_map{$node} = $pid;
	}else{
		print ">>> [err] failed to process the crawled pages on $node\n";
	}
}

if(scalar keys %node_pid_map){
	print ">>> wait for all processes to finish\n";
	waitClusterFinish(\%node_pid_map);
}

# extract asins
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


$pid = checkRemoteProcess($main_node, "crawler_console queryItem");
if(!$pid){
	$cmd = remoteCommand($main_node,"nohup crawler_console queryItem --asin-file=$remote_asin_file --offset-file=$response_linked_file --item-file=$item_file 1>$log_file 2>&1 & echo \$!");
	if(printAndExecute($cmd)){
		print ">>> failed to run amazon item profile query on $main_node\n";
		exit 1;
	}
}else{
	print ">>> [warn] amazon item query is running on $node: $pid";

}


# now generate the review page urls and distribute to cluster nodes
%node_pid_map = ();

for my $i (0 .. $#cluster_node_list){
	my $node = $cluster_node_list[$i];
	my $input_file = getFullRemotePath(LEAF_CATEGORY_DIR) . "/" .  FILE_SPLIT_PREFIX . $node_file_suffix_map[$i] . "_parsed.json";
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
