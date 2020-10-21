#! /bin/bash

# this script run ETL.py automatically

export laptop_dir="/home/ec2-user/aws_laptop_scrape"

cd $laptop_dir

nohup python3 src/ETL.py >> logging/collect_laptop_data.log 2>&1 &
