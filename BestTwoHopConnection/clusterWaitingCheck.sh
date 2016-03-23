#!/bin/sh
# Author: Abhijeet Sharma
# Version: 1.0
# Released: February 6, 2016
# Script for checking whether aws job is successfully completed or not.
# Program waits till job is completed and copies the respective cluster's logs once job is completed.
# Args: ClusterId, BucketName, Mean/Median/Fast

clusterid=$1
echo $clusterid
clusterStatus() {
	value=`aws emr list-clusters --cluster-states WAITING --query "Clusters[*].Id" --output text`
	if [ -w $value ]
	then
	 return 0
	else
	 return 1
	fi
}

while clusterStatus $1
do
sleep 20
done
echo "Complete"
