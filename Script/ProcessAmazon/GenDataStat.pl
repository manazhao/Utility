#!/usr/bin/perl
#
# generate data statistics 
use strict;
use warnings;
use Getopt::Long;

my $rating_file;
my $user_stat_file;
my $item_stat_file;
my $sum_file;

GetOptions("rating=s" => \$rating_file, "user-stat=s" => \$user_stat_file, "item-stat=s" => \$item_stat_file, "summary=s"=>\$sum_file) or die $!;

$rating_file and $user_stat_file and $item_stat_file and $sum_file or usage();

open my $rating_fh, "<" , $rating_file or die $!;
open my $user_fh, ">", $user_stat_file or die $!;
open my $item_fh, ">", $item_stat_file or die $!;
open my $sum_fh, ">", $sum_file or die $!;

my %user_map = ();
my %item_map = ();

my $num_ratings = 0;
my $num_users = 0;
my $num_items = 0;
while(<$rating_fh>){
    chomp;
    my ($uid,$iid,$r,@rest) = split /\t/;
    $user_map{$uid}++;
    $item_map{$iid}++;
    $num_ratings++;
}

$num_users = keys %user_map;
$num_items = keys %item_map;

my $sparsity = sprintf("%.8f",$num_ratings / ($num_users * $num_items) * 100);

print $sum_fh <<EOF;
# of users: $num_users
# of items: $num_items
# of ratings:   $num_ratings
sparsity:      $sparsity %
EOF

close $sum_fh; 

my %user_dist_map = ();
my %item_dist_map = ();

map {$user_dist_map{$_}++ } values %user_map;
map {$item_dist_map{$_}++ } values %item_map;

my $accu = 0;

foreach (sort {$a<=>$b} keys %user_dist_map){
    $accu += $user_dist_map{$_};
    print $user_fh join("\t",($_,$user_dist_map{$_}, $accu / $num_users)) . "\n";
}

$accu = 0;
foreach (sort {$a<=>$b} keys %item_dist_map){
    $accu += $item_dist_map{$_};
    print $item_fh join("\t",($_,$item_dist_map{$_}), $accu / $num_items) . "\n";
}

close $user_fh;
close $item_fh;
close $rating_fh;


sub usage{
    my $usage = <<EOF;
$0: [options]
    --rating    rating file
    --user-stat distribution over the number of ratings per user --item-stat distribution over the number of ratings per item
    --summary   summary file.e.g. data sparsity

EOF
    print $usage;
    exit(1);
}

