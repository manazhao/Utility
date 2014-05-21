#!/usr/bin/perl -w
#
use strict;
use warnings;

while(1){
	sleep 20;
	my $ps_result = `ps aux|grep detect_user_demo|wc -l`;
	if($ps_result == 2){
		# start new job
		print "start new batch detect job\n";
		`perl batch_detect_demo.pl`;
		last;
	}
}
