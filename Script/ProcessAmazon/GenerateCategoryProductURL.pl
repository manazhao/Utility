#!/usr/bin/perl
#
use strict;
use warnings;

while(<>){
    chomp;
    my $node = $_;
    map {print "http://amazon.com/b?node=$node&page=$_\n"} 1..5;
}
