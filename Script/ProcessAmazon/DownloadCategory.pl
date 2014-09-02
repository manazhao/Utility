#!/usr/bin/perl use Getopt::Long;
use strict;
use warnings;
use Getopt::Long;

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
use constant CATEGORY_PRODUCT_URL_FILE => LEAF_CATEGORY_DIR . "/category_product_url.csv";
use constant PRODUCT_DIR => "Product";
use constant USER_DIR => "User";
use constant REVIEW_DIR => "ProductReview";
use constant LOG_FILE => "run.log";


my @cluster_node_list = (
	"irkm-1",
	"irkm-2",
	"irkm-3",
	"irkm-4",
	"irkm-5",
	"irkm-6"
);

my $main_node = "irkm-1";

GetOptions("node-id=s" => \$node_id, "node-name=s" => \$node_name, "local-wd=s" => \$local_wd, "remote-wd=s" => \$remote_wd, "cluster-user=s" => \$cluster_user) or die $!;

$node_id  and $node_name and $local_wd and $remote_wd and $cluster_user or usage();

# get category path
$local_wd .= "/$node_name";

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

foreach my $sub(@sub_dirs){
	-d getFullLocalPath($sub) or mkdir getFullLocalPath($sub);
}

rsyncToCluster($local_wd, $remote_wd . "/");

# update remote wd to specific product category
$remote_wd .="/$node_name";

if(not remoteFileExist($main_node,getFullRemotePath(CATEGORY_TREE_FILE))){
	$cmd = remoteCommand($main_node, "crawler_console retrieveAmazonCategory --start-node=$node_id --result-file=" . getFullRemotePath(CATEGORY_TREE_FILE));
	printAndExecute($cmd);
}else{
	print ">>> category tree already exists: skip\n";
}

# generate category product page url
my $perl_cmd = 'perl -lne \'chomp; $n = $_; map {print \"http://amazon.com/b?node=$n&page=$_\"} 1..5\'';
$cmd = remoteCommand($main_node, "cat " . getFullRemotePath(CATEGORY_TREE_FILE) .  " |cut -f1 -d| | $perl_cmd > " . getFullRemotePath(CATEGORY_PRODUCT_URL_FILE));
if(not remoteFileExist($main_node,getFullRemotePath(CATEGORY_PRODUCT_URL_FILE))){
	printAndExecute($cmd);
}else{
	print ">>> category product url already exists: skip\n";
}

# now split the url list and distribute to other cluster nodes
if(not remoteFileExist($main_node, getFullRemotePath(CATEGORY_PRODUCT_URL_FILE))){
	print ">>> category product url file: " . getFullRemotePath(CATEGORY_PRODUCT_URL_FILE) . " does not exist, abort now! \n";
	exit(1);
}

# we need to copy to local working directory and split it
rsyncFromNode($main_node, getFullRemotePath(LEAF_CATEGORY_DIR), "$local_wd/");

# count lines
$cmd = "wc -l " . getFullLocalPath(CATEGORY_PRODUCT_URL_FILE);
my $num_of_urls = printAndExecute($cmd);
print ">>> number of category product url: $num_of_urls \n";
# split
my $split_size = int($num_of_urls/6) + 1;
$cmd = "split -l  $split_size " . getLocalRemotePath(CATEGORY_PRODUCT_URL_FILE);
printAndExecute($cmd);

# now distribute to all nodes
rsyncCluster(getFullLocalPath(LEAF_CATEGORY_DIR), "$remote_wd/");


sub remoteFileExist{
	my($host, $file) = @_;
	my $ssh_cmd = remoteCommand($host, "ls $file 2>&1");
	my $output = printAndExecute($ssh_cmd);
	if($output =~ m/No\s+such\s+file/){
		return 0;
	}
	return 1;
}

# download the category page

sub remoteCommand{
	my($host,$cmd) = @_;
	return "ssh $cluster_user\@$host \"$cmd\"";
}

sub executeResultCheck{
	my($output) = @_;
	if($output =~ m/No\s+such\s+file/){
		return 0;
	}
	return $output;
}

sub printAndExecute{
	my($cmd) = @_;
	print $cmd . "\n";
	my $cmdOutput = `$cmd`;
	chomp $cmdOutput;
	return $cmdOutput;
}

sub getFullRemotePath{
	my $path = @_;
	return $remote_wd . "/" . $path;
}

sub getFullLocalPath{
	my $path = @_;
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
	my ($cmd) = @_;
	foreach my $node(@cluster_node_list){
		my $sshCmd = remoteCommand($node,$cmd);
		printAndExecute($sshCmd);
	}
}


sub usage{
	print <<END;
	$0:
	--node-id	category Id
	--node-name	category name
	--local-wd	local working directory
	--remote-wd	remote working directory
	--cluster-user 	user to login cluster nodes (passwordless ssh access is required)
END
	die $!;
}
