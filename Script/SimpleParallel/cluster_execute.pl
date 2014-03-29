#!/usr/bin/perl
use strict;
use warnings;
use Cwd;
use Sys::Hostname;
use Data::Dumper;
use Getopt::Long;

# local directory  where the node scripts are held. They will be shipped to the nodes
my $script_src = '';
# directory on the remote server where the scripts will be shipped to
my $script_dst = '';
my $help = 0;
my @node_list = (
	"irkm-1",
	"irkm-2",
	"irkm-3",
	"irkm-4",
	"irkm-5",
	"irkm-6"
);

GetOptions(
	"script-src=s" => \$script_src,
	"script-dst=s" => \$script_dst,
	"help" => \$help
) or die("error in the command line arguments\n");

if($script_src eq '' or $script_dst eq '' or $help){
	usage();
	exit(1);
}

my $task_cmd = "bash main.sh";

-d $script_src or die "the source folder does not exist, please check\n";
#-f "$script_src/main.sh" or die "the task file [main.sh] does not exist, please create it\n";

foreach(@node_list){
	# check the existence of destination folders
	# need to create if not exists
	my $ssh_ls_cmd = "ssh manazhao@" . $_ . " ls $script_dst";
	# redirect the stderr to stdout
	my $ssh_ls_result = `$ssh_ls_cmd 2>&1`;
	if($ssh_ls_result =~ m/No such/){
		print "try to create the target folder on: $_\n";
		my $ssh_mkdir_cmd = "ssh manazhao@" . $_ . " mkdir -p $script_dst";
		print $ssh_mkdir_cmd . "\n";
		my $mkdir_result = `$ssh_mkdir_cmd 2>&1`;
		print $mkdir_result ;
	}
	my $dst_path = "manazhao@" . $_ . ":$script_dst";
	my $rsync_cmd = "rsync -avuz  $script_src/* " . "manazhao@" . "$_:$script_dst";
	print $rsync_cmd . "\n";
	`$rsync_cmd`;
	# execute remote command
	my $ssh_host = "ssh manazhao@" . $_;
	my $remote_cmd = "cd $script_dst; nohup $task_cmd >nohup.out 2>&1 &";
	my $remote_ssh_cmd = $ssh_host . " \"$remote_cmd\"";
	print $remote_ssh_cmd . "\n";
	`$remote_ssh_cmd`;
}

sub usage{
print <<END;
Usage: $0 
       --script-src	source folder of the script
       --script-dst	destination folder on the node where the script will be shipped to	
       --help		print the help message
END
}
