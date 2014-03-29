#!/usr/bin/perl

#
# this program is used to detect human face and estimate the age and gender in a further step.
# 	** face detection is done through opencv face detection utility and 
# 	** age and gender estimation are done through OpenBR. 
#


use strict;
use warnings;
use Cwd;
use File::Basename;
use Getopt::Long;
use JSON qw(encode_json);

my $input_photo_dir = '';
my $output_file = '';
my $help = 0;
GetOptions(
	"input-dir=s" => \$input_photo_dir,
"output-file=s" => \$output_file,
	"help" => \$help
) or die "error with getting command arguments\n";

if($input_photo_dir eq '' or $output_file eq '' or $help){
	usage();
	exit(-1);
}

-d $input_photo_dir or die "input folder does not exist";
my $output_dir = dirname $output_file;
-d $output_dir or `mkdir -p $output_dir`;
-d $output_dir or die "failed to make the folder for the result file\n";


# face detection command
my $fd_bin = "/home/qzhao2/git/Utility/FaceDetection/FaceDetection.out";
my $fd_cwd = dirname $fd_bin;


# setup the file that will contain the demographic information
open OUT_FILE, ">", $output_file or die $!;
# list the files under the input folder
opendir DIR, $input_photo_dir or die $!;
while(my $file = readdir(DIR)){
	next if ($file =~ m/^\./);
	my $cwd = getcwd;
	# switch to face detection binary directory as the cascade files are there
	chdir $fd_cwd;
	my $img_path = $input_photo_dir . "/" . $file;
	my $fd_status = system($fd_bin . " $img_path");
	# switch back to previous working directory
	chdir $cwd;
	if($fd_status){
		# further estimate the age and gender
		my %author_demo = ("url" => $img_path, 
			"gender" => '',
			"age" => ''
		);
		my $gender_cmd = "br -algorithm GenderEstimation -enroll $img_path  meta.csv";
		my $gender_cmd_result = `$gender_cmd`;
		if( not open META_FILE, "<", "meta.csv" ){
			print  "gender estimation error with: $img_path\n";
		}else{
			# read the result
			# skip the head
			<META_FILE>;
			my $line = <META_FILE>;
			my @fields = split /,/, $line;
			# the gender is 18-th column
			my $gender = $fields[17];
			$author_demo{"gender"} = $gender;
			close META_FILE;
		}

		my $age_cmd = "br -algorithm AgeEstimation -enroll $img_path  meta.csv";
		my $age_cmd_result = `$age_cmd`;
		if( not open META_FILE, "<", "meta.csv" ){
			print STDERR "age estimation error with: $img_path\n";
		}else{
			# read the result
			# skip the head
			<META_FILE>;
			my $line = <META_FILE>;
			my @fields = split /,/, $line;
			# the gender is 6-th column
			my $gender = $fields[5];
			$author_demo{"age"} = $gender;
			close META_FILE;
		}
		my $demo_json = encode_json(\%author_demo);
		print OUT_FILE $demo_json . "\n";
	}
}
close OUT_FILE;

sub usage{
print qq{
Usage : $0
--input-dir	folder where photos are located
--output-file	file where the extracted age, gender information goes to
--help print this usage message
};
}
