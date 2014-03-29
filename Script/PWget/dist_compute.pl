#!/usr/bin/perl
use strict;
use warnings;
use Cwd;
use Sys::Hostname;
use Data::Dumper;

# define how to map files to host
my %host_file_pat_map = (
			"irkm-1" => ["urlaa","*.pl"],
			"irkm-2" => ["urlab","*.pl"],
			"irkm-3" => ["urlac","*.pl"],
			"irkm-4" => ["urlad","*.pl"],
			"irkm-5" => ["urlae","*.pl"],
			"irkm-6" => ["urlaf","*.pl"],
			);


my $host_name =  hostname;
my $cwd = getcwd;
my $task_cmd = "perl wget.pl";

foreach(keys %host_file_pat_map){
	($_ eq $host_name) and next;
	# check the existence of destination folders
	# need to create if not exists
	my $ssh_ls_cmd = "ssh manazhao@" . $_ . " ls $cwd";
	# redirect the stderr to stdout
	my $ssh_ls_result = `$ssh_ls_cmd 2>&1`;
	if($ssh_ls_result =~ m/No such/){
		print "try to create the target folder on: $_\n";
		my $ssh_mkdir_cmd = "ssh manazhao@" . $_ . " mkdir -p $cwd";
		print $ssh_mkdir_cmd . "\n";
		my $mkdir_result = `$ssh_mkdir_cmd 2>&1`;
		print $mkdir_result ;
	}
	my $file_pats = $host_file_pat_map{$_};
	my $dst_path = "manazhao@" . $_ . ":$cwd";
	foreach(@{$file_pats}){
		my $src_path = "$cwd/$_";
		my $rsync_cmd = "rsync -avuz  $src_path $dst_path";
		print $rsync_cmd . "\n";
		`$rsync_cmd`;
	}
	# execute remote command
	my $ssh_host = "ssh manazhao@" . $_;
	my $remote_cmd = "cd $cwd; nohup $task_cmd >nohup.out 2>&1 &";
	my $remote_ssh_cmd = $ssh_host . " \"$remote_cmd\"";
	print $remote_ssh_cmd . "\n";
	`$remote_ssh_cmd`;
}

