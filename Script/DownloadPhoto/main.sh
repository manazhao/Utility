#!/bin/bash

perl extract_photo_url.pl >extract.log 2>&1 &
perl download_photo.pl >wget.log 2>&1 &
