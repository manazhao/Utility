#!/usr/bin/perl

#USE DECLARATIONS
use strict;
use warnings;
use WWW::Mechanize;
use Term::ANSIColor;

#VARIABLE DECLARATIONS
my $mech = WWW::Mechanize->new();
my $img;
my $title;
my $pic_page;
my $url;
my $count;
my @links;

#CONNECT TO FACEBOOK
$url = 'https://www.facebook.com/';
$mech = WWW::Mechanize->new();
$mech->agent_alias( 'Linux Mozilla' );
$mech->get( $url );
$title = $mech->title();

#LOGIN FORM
print "Connected to Facebook.\n";
print "Logging in...";
$mech->form_id("login_form");
$mech->field("email",'zhaoqi@fudan.edu.cn');
$mech->field("pass",'234346543');
$mech->click();
print "done!\n";

#NAVIGATE TO USER PAGE
$mech->get("https://www.facebook.com/Kidamer/about");
$title = $mech->title();
print "Finding $title 's profile pictue...\n";

#FIND PROFILE PICTURE
#$img = $mech->find_image(url_regex => qr/s160x160/, );
#print $img->url();
#downloadImage($img->url(),$mech->title().".jpg");
my $content = $mech->content();
print $content;

sub downloadImage
{
    my $local_file_name = $_[1];
    my $b = WWW::Mechanize->new;
    print "Downloading: $_[1]...";
    $b->get( $_[0], ":content_file" => $local_file_name );
    print "done!\n";
}
