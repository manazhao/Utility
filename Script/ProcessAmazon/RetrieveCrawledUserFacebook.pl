#!/usr/bin/perl
#
use strict;
use warnings;
use Getopt::Long;

my $fb_email;
my $fb_pwd;
my $git_home ;
my $work_dir;

GetOptions("email=s" => \$fb_email, "password=s" => \$fb_pwd, "git-home=s" => \$git_home,"wd=s"=>\$work_dir) or die $!;
$fb_email and $fb_pwd and $git_home and $work_dir or usage();
-d $work_dir or die "working directory: $work_dir does not exist\n";
# switch to working directory
chdir $work_dir;
# settings
my $fb_program = "$git_home/Utility/Script/ProcessAmazon/InteractFacebook.pl";
-f $fb_program or die "InteractFacebook.pl takes the wrong path:$!";
# file that maps amazon user id to facebook url
my $author_fb_file = `ls *author_profile_facebook.csv 2>/dev/null`;
chomp $author_fb_file;
die "author facebook file not exist:$!" if !$author_fb_file;
# get Facebook user name for each url
my $tmp_file = ".tmp_file";
# get facebook user name
my $cmd = "cut -f2 -d, $author_fb_file |perl $fb_program --email=$fb_email --password=$fb_pwd --task getUserName > $tmp_file";
print ">>> $cmd \n";
`$cmd`;

# try to join amazon user id and facebook user name
my $author_fb_uname_file = 'author_profile_facebook_uname.csv';
$cmd = "paste -d, $author_fb_file $tmp_file | perl -nle 'print unless /\\,\$/' |cut -f1,3 -d, > $author_fb_uname_file";
print ">>> $cmd \n";
`$cmd`;
# now get the about page and download it
my $about_page_file = "author_profile_facebook_about_pages.csv";
my $page_dir = "facebook_pages";
-d $page_dir or mkdir $page_dir;

$cmd = "cut -f2 -d, $author_fb_uname_file |perl $fb_program --email=$fb_email --password=$fb_pwd --page-dir=$page_dir --task getAbout > $tmp_file ";
print ">>> $cmd \n";
`$cmd`;

print ">>> done!\n";


sub usage{
	my $usage = <<EOF;
usage: $0 [options]
	--email		facebook email
	--password	facebook password
	--git-home	git project home directory
	--wd		working directory where the files are hosted
	
EOF
	die $usage;

}
