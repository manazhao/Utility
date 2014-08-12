#!/usr/bin/perl
#
# generate a list of amazon profile urls by gathering amazon user ids from difefrent product categories
# the steps are,
# 1) find the files containing user ids on different machines
# 2) move the files to the master machine (the one executes this script)
# 3) concatenate all files obtained above and take the uniq items
# 4) find the files which contain the crawled user profile ids
# 5) copy to the master server and concatenate and uniq
# 6) take the difference of the list generated in step 3 and 5 

use strict;
use warnings;
use File::Basename;



sub _concatenate_files{
	my ($local_folder,$files) = @_;
	my $cat_result_file = 

}

sub _copy_files_to_master{
	my($ssh_user, $host_list, $target_path, $file_pattern, $dst_path) = @_;
	my @copied_files = ();
	foreach my $host(@$host_lists){
		my $find_cmd = _compose_ssh_cmd($user,$host, " 'find $target_path -type f |grep $file_pattern'"); 
		print ">>> $find_cmd\n";
		my $find_result = `$find_cmd 2>/dev/null`;
		my @files = split /\n/, $find_result;
		# copy each of them to current host
		foreach my $remote_file(@files){
			my $file_name = basename($remote_file);
			my $dst_file_name = "from_host_$host" . "_" .  $file_name;
			my $scp_cmd = _compose_scp_cmd($user,$host, $remote_file,$dst_path . "/$file_name");
			print ">> $scp_cmd\n";
			`$scp_cmd`;
			push @copied_files, $file_name;
		}
	}
	return \@copied_files;
}

sub _compose_ssh_cmd{
	my($user,$host,$remote_cmd) = @_;
	return "ssh $user@" . $host . " $remote_cmd";
}

sub _comps_scp_cmd{
	my($user,$host,$remote_path,$local_path) = @_;
	return "scp -r $user@" . "$host:$remote_path $local"
}

