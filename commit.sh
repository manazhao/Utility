#!/bin/bash

if [ $# != 1 ]; then 
 echo "commit message cannot be empty"
 echo "usage: ./commit [your message]"
 exit 1;
fi
message=$1;

echo "stage file changes"
git add -A
echo "commit changes"
git commit -m "$message"
echo "push changes to remote repository"
git push origin
