#!/usr/bin/perl
#
# download facebook page with a login account
#
use strict;
use warnings;
use Getopt::Long;

my $fb_email;
my $fb_pwd;
my $task;
my $page_dir = "./";

GetOptions("email=s" => \$fb_email, "password=s" => \$fb_pwd, "task=s" => \$task, "page-dir=s" => \$page_dir) or die $!;
$fb_email and $fb_pwd and $task or usage();

my %task_list = (
	"getUserName" => "get user meta information like demographic information and interest",
	"getAbout" => "get information from about page"
);

die "unsupoorted task: $!" unless $task_list{$task};


# curl parameters
my $cookies = '/tmp/cookies.txt';
my $user_agent = "Firefox/3.5";
my $verbose = "";

# first login facebook
login($fb_email,$fb_pwd);
# now read from the command line

while(<>){
	chomp;
	my $line = $_;
	next if $line eq "";
	if($task eq "getUserName"){
		my $response = download_url($line);
		# find the about linke
		if($response =~ m/href=".*?\/([^\/]+?)\/about"\s+data\-medley\-id="pagelet_timeline_medley_about"/g){
			print $1."\n";
		}else{
			print "\n";
		}
	}elsif( $task eq "getAbout"){
		# get about page
		my $aboutUrl = "https://facebook.com/$line/about";
		my $aboutPage = download_url($aboutUrl);
		# replace \n
		$aboutPage =~ s/\n//g;
		$aboutPage =~ s/\s+/ /g;
		# write to file
		my $file_name = $page_dir . "/" . $line . "_about.html";
		open my $tmp_fh, ">" ,$file_name or die $!;
		print $tmp_fh $aboutPage;
		close $tmp_fh;
	}else{
		die "unsupported task: $!";
	}
}


sub login{
	my($email,$pwd) = @_;
	`curl -X GET 'https://www.facebook.com/home.php'  $verbose --user-agent $user_agent --cookie $cookies --cookie-jar $cookies --location 2>/dev/null`;
	`curl -X POST 'https://login.facebook.com/login.php' $verbose --user-agent $user_agent --data-urlencode "email=$email" --data-urlencode "pass=$pwd" --cookie $cookies --cookie-jar $cookies 2>/dev/null`;
}

sub download_url{
	my($url) = @_;
	my $response = `curl -X GET '$url' $verbose --user-agent $user_agent --cookie $cookies --cookie-jar $cookies --location 2>/dev/null`;
	return $response;
}

sub usage{
	my $usage =  <<EOF;
usage $0 [options]
	--email		Facebook email
	--password	Facebook password
	--task		task to perform: getUserName, getAbout
EOF
	print $usage . "\n";
	exit(1);

}
