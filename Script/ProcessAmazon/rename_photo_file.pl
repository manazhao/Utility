#!/usr/bi/perl

# rename photo files by replacing , with _
use strict;
use warnings;

while(<STDIN>){
	chomp;
	my $file = $_;
	-f $file  or die "file: $_ not exist\n";
	if($file =~ m/,/){
		my $new_file;
		$new_file = $file;
		$new_file =~ s/,/_/g;
		my $cmd = "mv $file $new_file";
		print $cmd . "\n";
		`$cmd`;
	}
}

