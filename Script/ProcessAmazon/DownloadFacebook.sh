#!/bin/bash
 
# do the following tasks,
# 1) get facebook  user name given user's facebook profile url extracted from amazon author profile page
# 2) download  user Facebook about page and save it to harddrive
# 3) run parser to extract Facebook basic information and user's likes


# file that maps amazon user id to facebook profile url
AUTHOR_FB_FILE=`ls *author_profile_facebook.csv`
# need to get user's Facebook user name so that we can form the about page url

perl /home/tmp/manazhao/git/Utility/Script/ProcessAmazon/InteractFacebook.pl --

