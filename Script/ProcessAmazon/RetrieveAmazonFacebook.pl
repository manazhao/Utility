#!/usr/bin/perl
#
use strict;
use warnings;
use Getopt::Long;

my $fb_email;
my $fb_pwd;
my $work_dir;
my $target;

GetOptions("email=s" => \$fb_email, "password=s" => \$fb_pwd, "wd=s"=>\$work_dir, "target=s" => \$target) or die $!;
$fb_email and $fb_pwd  and $work_dir and $target or usage();
-d $work_dir or die "working directory: $work_dir does not exist\n";
# switch to working directory
chdir $work_dir;
# settings
my $fb_program = "InteractFacebook.pl";

my $target_parsed_file = $target . "_parsed.json";
-f $target_parsed_file or die "the target file - $target_parsed_file does not exist:$!";

my $log_file = $target . "_retrieve_fb.log";
my $target_site_file = $target. "_site.csv";

# redirecting the erorr to the log file 
my $cmd = "JSON2CSV.pl --json=$target_parsed_file --field='id,site' --csv=$target_site_file 1>$log_file 2>&1 ";

if(not -f $target_site_file){
	print ">>> $cmd\n";
	`$cmd`;	
}else{
	print ">>> $target_site_file already exists, skip\n";
}

# file containing the "site" attribute of $target
my $target_fbsite_file = $target . "_fbsite.csv";

# extract authors with facebook link as their site url
if(not -f $target_fbsite_file){
	$cmd = "grep -P 'facebook.com' $target_site_file > $target_fbsite_file";
	print ">> $cmd\n";
	`$cmd`;
}else{
	print ">>> facebook site file - $target_fbsite_file already exists, skip\n";
}

# ensure the fb field file exists
-f $target_fbsite_file or die "facebook field file - $target_fbsite_file does not exist:$!";

# try to join amazon user id and facebook user name
# remove amazon authors which does not have a facebook user name
my $target_fb_uname_file = $target . "_fb_uname.csv";
if(not -f $target_fb_uname_file){
# get Facebook user name for each url
	my $tmp_file = ".tmp_file";
# get facebook user name
	my $cmd = "cut -f2 -d, $target_fbsite_file | $fb_program --email=$fb_email --password=$fb_pwd --task getUserName > $tmp_file";
	print ">>> $cmd \n";
	`$cmd`;
# 
	$cmd = "paste -d, $target_fbsite_file $tmp_file | perl -nle 'print unless /\\,\$/' |cut -f1,3 -d, > $target_fb_uname_file";
	print ">>> $cmd\n";
	`$cmd`;
}else{
	print ">>> facebook username file - $target_fb_uname_file already exists, skip\n";
}

print ">>> start to download facebook user information\n";
if(not -f $target_fb_uname_file){
# now get the about page and download it
	my $page_dir = "facebook_profile_pages";
	-d $page_dir or mkdir $page_dir;
	$cmd = "cut -f2 -d, $target_fb_uname_file |perl $fb_program --email=$fb_email --password=$fb_pwd --page-dir=$page_dir --task getAbout 1>$log_file 2>&1  ";
	print ">>> $cmd \n";
	`$cmd`;
	print ">>> done!\n";
}


sub usage{
	my $usage = <<EOF;
usage: $0 [options]
	--email			facebook email
	--password		facebook password
	--wd			working directory where the files are hosted
	--target		target name, here it's parsed amazon profile

EOF
		   die $usage;
}
