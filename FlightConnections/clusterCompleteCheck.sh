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
	value=`aws emr list-clusters --terminated --query "Clusters[*].{Ids:Id, codes:Status.StateChangeReason.Code}[?Ids=='$clusterid' && codes=='ALL_STEPS_COMPLETED'].codes" --output text`
	#echo $value
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
#echo -e `aws s3 sync s3://$2/logs/$clusterid/hadoop-mapreduce/history logs/$3`
echo "Complete"
