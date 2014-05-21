#!/usr/bin/perl
#
use strict;
use warnings;

my %server_file_map = (
	"irkm-1" => ["AmazonBeautyData.tar.gz","AmazonToyGameData.tar.gz", "AmazonToyGameData_part2.tar.gz"],
	"irkm-2" => ["HealthPersonalData.tar.gz", "AmazonGroceryGourmetFoodData.tar.gz"],
	"irkm-3" => ["AmazonMovieData.tar.gz", "AmazonKindleStoreData.tar.gz", "AmazonElectronicData.tar.bz2"],
	"irk-4" => ["AmazonShoeData.tar.gz", "AmazonClothingAccessoriesData.tar.bz2"]
);


my $source_dir = "/media/020C57B30C57A109/AmazonCrawledData";
my $dst_dir = "/home/tmp/manazhao/amazon";


# copy files to the host

foreach my $host(keys %server_file_map) {
	my $files = $server_file_map{$host};
	foreach my $file(@$files){
		my $src_file = join("/", ($source_dir, $file));
		-f $src_file or die "source folder does not exist:$!";
		my $dst_file = join("/", ($dst_dir, $file));
		# test existence of destination folder
		my $cmd = "ssh manazhao@" . $host .  " 'ls $dst_dir'";
		my $output = `$cmd 2>&1`;
		if($output =~ m/No such/g){
			print STDERR ">>> Warning: create folder: $dst_dir on $host\n";
			$cmd = "ssh manazhao@" . $host . " 'mkdir -p $dst_dir' ";
			print $cmd . "\n";
			`$cmd`;
		}
		# test the existence of the destination file, skip if exist
		$cmd = "ssh manazhao@" . $host . " 'ls $dst_file' ";
		print $cmd . "\n";
		$output = `$cmd 2>&1 `;
		if($output =~ m/No such/g){
			# make the copy 
			$cmd = "scp $src_file manazhao@" . $host . ":$dst_file >> $host.log 2>&1";
			print $cmd . "\n";
			 `$cmd`;
		}else{
			# skip overwrite the existing file
			print STDERR ">>> Warning: $dst_file exists on $host, copy skipped\n";
		}
	}
}

