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
my $LETTER_SEQ = [qw(aa ab ac ad ae af ag ah ai aj ak al am an)];

my @node_file_suffix_map = qw(aa ab ac ad ae af);
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
	if(not $cluster_manager->local_file_exist($sub, is_dir => 1)){
		$cluster_manager->execute_on_local("mkdir -p $sub");
		$cluster_manager->execute_on_cluster(
			cmd_pattern => "[ ! -d $sub ] && mkdir -p $sub"
		);
	}
}

my $main_node = 0;
my $cmd;
my $message;
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
print "[info] wait category query finish\n";
my $node_pid_map = $cluster_manager->check_node_pname($main_node, "crawler_console\\s+retrieveAmazonCategory");
if($node_pid_map){
	$cluster_manager->wait_node_pid($main_node,$node_pid_map);
}

#######################3 generate leaf category file
print "[info] generate leaf category list\n";
$cmd = 'perl -lne "chomp;  if(/(^[^\t]+)\t$/){print \$1}"  ' . CATEGORY_TREE_FILE . " > " . LEAF_CATEGORY_FILE;
if(not $cluster_manager->node_file_exist($main_node,LEAF_CATEGORY_FILE)){
	$cluster_manager->execute_on_node($main_node,cmd_pattern => $cmd);
}else{
	print "[info] " . LEAF_CATEGORY_FILE. " already exists\n";
}


################ now  generate category product url
print ">>> generate category product page url\n";
$cmd = "cat " . LEAF_CATEGORY_FILE . " |cut -f1 -d\\| | GenerateCategoryProductURL.pl > " . CATEGORY_PRODUCT_URL_FILE;
if(not $cluster_manager->node_file_exist($main_node,CATEGORY_PRODUCT_URL_FILE)){
	$cluster_manager->execute_on_node($main_node,cmd_pattern => $cmd);
}else{
	print "[info] " . CATEGORY_PRODUCT_URL_FILE. " already exists\n";
}

############# copy file to local
print ">>> copy product url file to local\n";
if(not $cluster_manager->local_file_exist($main_node,CATEGORY_PRODUCT_URL_FILE,echo=>0)){
	$cluster_manager->rsync_from_node(
		remote_path_pattern => CATEGORY_PRODUCT_URL_FILE,
		local_path => LEAF_CATEGORY_DIR
	);
}else{
	print "[info] " . CATEGORY_PRODUCT_URL_FILE. " already exists\n";
}

############### split the url list and distribute to cluster
print ">>> split url and distribute to cluster\n";
my $tmp_result = $cluster_manager->execute_on_cluster(
	cmd_pattern => "[ -f " . LEAF_CATEGORY_DIR . "/x%s ] && echo 1",
	cmd_args => $LETTER_SEQ,
	echo => 0
);

if(not $cluster_manager->all_nodes_true($tmp_result)){
	$cluster_manager->split_and_distribute(
		CATEGORY_PRODUCT_URL_FILE,
		LEAF_CATEGORY_DIR,
		split_prefix => "x",
	);
}else{
	print "[info] " . CATEGORY_PRODUCT_URL_FILE. " splits already exist\n";
}

############ download the pages
my $cluster_pid_map;
print ">>> start to download category product pages\n";
$tmp_result = $cluster_manager->execute_on_cluster(
	cmd_pattern => "[ -d " . LEAF_CATEGORY_DIR . "/x%s_pages ] && echo 1",
	cmd_args => $LETTER_SEQ
);
if(not !$cluster_manager->all_nodes_true($tmp_result)){
	$cluster_manager->execute_on_cluster(
		cmd_pattern => "mkdir -p " . LEAF_CATEGORY_DIR . "/x%s_pages",
		cmd_args => $LETTER_SEQ
	);
	my $log_file = LEAF_CATEGORY_DIR . "/nohup_wget.log";
	$cluster_pid_map = $cluster_manager->execute_on_cluster_bg(
		cmd_pattern => "wget.pl --url-file=" . LEAF_CATEGORY_DIR . "/x%s --wait=3",
		cmd_args => $LETTER_SEQ,
		log_file => $log_file
	);	
}else{
	print "[info] category pages already exist\n";
}

print ">>> wait for category product pages downloading\n";
$cluster_pid_map = $cluster_manager->check_cluster_pname("wget.pl");
$cluster_manager->wait_cluster_pid($cluster_pid_map);

################ process downloaded category product pages

print ">>> parse the category product pages\n";
$tmp_result = $cluster_manager->execute_on_cluster(
	cmd_pattern => "[ -f " . LEAF_CATEGORY_DIR . "/x%s_parsed.json ] && echo 1",
	cmd_args => $LETTER_SEQ
);

if(not $cluster_manager->all_nodes_true($tmp_result)){
	$cluster_manager->sync_cluster_execute(
		cmd_pattern => "ProcessCrawledPage.pl --wd=LeafCategory --target=x%s --parser=Amazon/CategoryPageParser",
		cmd_args => $LETTER_SEQ
	);
}else{
	print "[info] category pages are already parsed\n";
}

$cluster_pid_map = $cluster_manager->check_cluster_pname("CategoryPageParser");
$cluster_manager->wait_cluster_pid($cluster_pid_map);

###################### concatenate the parsed category product result
print ">>> concatenate the parsed category product pages and upload to main node\n";
if(not $cluster_manager->node_file_exist($main_node, LEAF_CATEGORY_DIR . "/category_item_profile.json")){
	my $merged_file = LEAF_CATEGORY_DIR . "/category_item_profile.json";
	$cluster_manager->cluster_cat(
		remote_path_pattern => LEAF_CATEGORY_DIR . "/x%s_parsed.json",
		remote_path_args => $LETTER_SEQ,
		local_path => $merged_file
	);
	$cluster_manager->rsync_to_node(
		$main_node, $merged_file, LEAF_CATEGORY_DIR
	);
}else{
	print "[info] category page parsed result are already merged\n";
}


############### extract product id and generate the review page file
# generate the item -> number of reviews file and concatenate to a single file
if(not $cluster_manager->all_nodes_true($cluster_manager->execute_on_cluster( cmd_pattern => "[ -f " . LEAF_CATEGORY_DIR . "/x%s_asin_rp.csv ] && echo 1", cmd_args => $LETTER_SEQ))){
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

############## start to download product profile
print ">>> start to download product profile\n";
my $asin_file = PRODUCT_DIR . "/asin.csv";
if(not $cluster_manager->node_file_exist($main_node, PRODUCT_DIR . "/response_linked.csv") ){
	print ">>> start to query amazon item profile on node-0\n";
	my $response_offset_file = PRODUCT_DIR . "/response_pos.csv";
	my $response_linked_file = PRODUCT_DIR . "/response_linked.csv";
	my $item_file = PRODUCT_DIR . "/profile.json";
	my $log_file = PRODUCT_DIR . "/nohup_api.log";
	my $cmd = "nohup crawler_console queryItem --asin-file=$asin_file --offset-file=$response_offset_file --response-file=$response_linked_file --item-file=$item_file";
	$cluster_manager->execute_on_node_bg(0, $cmd, log_file => $log_file);
}

my $query_item_pid = $cluster_manager->check_node_pname(0,"crawler_console\\s+queryItem");
if($query_item_pid){
	print "[info] product profile downloading is ongoing\n";
}

# start to download the review pages
if(not $cluster_manager->all_nodes_true($cluster_manager->execute_on_cluster( cmd_pattern => "[ -d " . REVIEW_DIR . "/x%s_pages ] && echo 1", cmd_args => $LETTER_SEQ))){
	print "[info] start to download review pages\n";
	$cluster_pid_map = $cluster_manager->execute_on_cluster_bg(
		cmd_pattern => "crawler_console downloadReview --input=" . REVIEW_DIR . "/x%s --wait=2",
		cmd_args => $LETTER_SEQ
	);
}

################# start to parse review pages after all pages are downloaded
$cluster_pid_map = $cluster_manager->check_cluster_pname("crawler_console\\s+downloadReview");
print "[info]: wait for review downloading finish\n";
$cluster_manager->wait_cluster_pid($cluster_pid_map);

########################## parse the review pages
print ">>> start to parse the review pages\n";
$tmp_result = $cluster_manager->execute_on_cluster(
	cmd_pattern => "[ -f " . REVIEW_DIR . "/x%s_linked.txt ] && echo 1",
	cmd_args => $LETTER_SEQ
);
if(not $cluster_manager->all_nodes_true($tmp_result)){
	$cluster_manager->sync_cluster_execute(
		cmd_pattern => "ProcessCrawledPage.pl --wd=ProductReview --target=x%s --parser=Amazon/ProductReviewXMLParser",
		cmd_args => [$LETTER_SEQ,$LETTER_SEQ]
	);
}else{
	print "[info] product reviews are already parsed\n";
}

print ">>> wait for review parsing to finish\n";
$cluster_pid_map = $cluster_manager->check_cluster_pname("ProcessCrawledPage.pl.*?ProductReviewXMLParser");
if($cluster_pid_map){
	$cluster_manager->wait_cluster_pid($cluster_pid_map);
}

################## now concatenate all parsed review files and put to irkm-1
print "[info] concatenate all review files and upload to irkm-1\n";
if(not $cluster_manager->node_file_exist($main_node, REVIEW_DIR . "/review.json")){
	my $merged_file = REVIEW_DIR . "/review.json";
	$cluster_manager->cluster_cat(
		remote_path_pattern => REVIEW_DIR . "/x%s_parsed.json",
		remote_path_args => $LETTER_SEQ,
		local_path => $merged_file
	);
	print "[info] upload review file to node-0\n";
	$cluster_manager->rsync_to_node(0,$merged_file, REVIEW_DIR . "/");
}else{
	print "[warn] parsed reviewed data is already concatenated\n";
}

########### just run some cleanning work

########## separate the review text and rating
print ">>> separate review and rating from the parsed review data\n";
if(not $cluster_manager->node_file_exist($main_node, REVIEW_DIR . "/review_text.json")){
	my $cmd = "FlattenReviewJSON.pl --json=" . REVIEW_DIR . "/review.json --rate=" . REVIEW_DIR . "/review_rate.csv --review=" . REVIEW_DIR . "/review_text.csv";
	$cluster_manager->execute_on_node($main_node, $cmd);
}else{
	print "[info] review is already separted\n";
}

######### backup the data to local hard drive and cloud device
######### only for those utilmate result
my @bk_file_list = (
	PRODUCT_DIR . "/asin.csv",
	PRODUCT_DIR . "/item_profile.json",
	PRODUCT_DIR . "/response_linked.txt",
	PRODUCT_DIR . "/response_pos.csv",
	PRODUCT_DIR . "/item_profile.json",
	LEAF_CATEGORY_DIR . "/category_item_profile.json",
	LEAF_CATEGORY_DIR . "/leaf_category.csv",
	CATEGORY_DIR . "/category_tree.csv",
	REVIEW_DIR . "/review.json",
	REVIEW_DIR . "/review_rating.csv",
	REVIEW_DIR . "/review_text.csv"
);

my $local_bk_dir = "/home/qzhao2/irkmwdex4-nfs/AmazonParsed/Ultimate/$node_name/";
$cluster_manager->init_local_wd($local_bk_dir);
foreach my $file (@bk_file_list){
	$cluster_manager->rsync_from_node(
		$main_node,$file,$local_bk_dir);
}

########### all done!!!!
print ">>>>>>>>>>>>>>>>>>>>>>>> ALL DONE!!!! >>>>>>>>>>>>>>>>>>>>>>>>>\n";


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
