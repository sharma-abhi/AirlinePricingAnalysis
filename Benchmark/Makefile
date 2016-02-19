#!bin/bash
# Makefile for Map Reduce A3 project.
# Sharma, Abhijeet and Khan, Afan Ahmad

# CONFIGURATION PARAMETERS
# Single/Multi Threading
localInput=/media/Windows/all

# Pseudo-Distributed Hadoop
jarName=cc.jar
hadoopVersion=2.7.2
hadoopDriver=CarrierCount
hdfsRoot=/user/abhijeet
hdfsInput=input
localOutput=output
hdfsOutput=output

# Cloud
awsRegion=us-east-1a
awsInstance=m1.medium 
awsBucketName=bucketforabhia3
awsInputDir=input
awsOutputDir=output
awsLogDir=logs

single-mean:
	mkdir -p output
	mkdir -p time
	javac -cp javacsv.jar Solution.java FileThread.java FlightInfo.java
	{ time java -classpath javacsv.jar: Solution -input=${localInput} mean;} 2> time/time_threading_single_mean.txt

single-median:
	mkdir -p output
	mkdir -p time
	javac -cp javacsv.jar Solution.java FileThread.java FlightInfo.java
	{ time java -classpath javacsv.jar: Solution -input=${localInput} median;} 2> time/time_threading_single_median.txt

multi-mean:
	mkdir -p output
	mkdir -p time
	javac -cp javacsv.jar Solution.java FileThread.java FlightInfo.java
	{ time java -classpath javacsv.jar: Solution -p -input=${localInput} mean;} 2> time/time_threading_multi_mean.txt

multi-median:
	mkdir -p output
	mkdir -p time
	javac -cp javacsv.jar Solution.java FileThread.java FlightInfo.java
	{ time java -classpath javacsv.jar: Solution -p -input=${localInput} median;} 2> time/time_threading_multi_median.txt

format: 
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver
	
hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh

hadoop-setup:
	hadoop fs -mkdir -p ${hdfsRoot}
	hadoop fs -mkdir -p ${hdfsRoot}/${hdfsInput}

hadoop-upload:
	hadoop fs -put ${localInput}/*.gz ${hdfsRoot}/${hdfsInput}

cloud-setup:
	aws s3 mb s3://${awsBucketName}

cloud-upload:
	#aws s3 cp ${localInput}/*.gz s3://${awsBucketName}/${awsInputDir}
	aws s3 sync ${localInput} s3://${awsBucketName}/${awsInputDir}

pseudo-mean:
	mkdir -p time
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${hadoopDriver}.java
	jar cf ${jarName} *.class
	hadoop fs -rm -r -f ${hdfsOutput}
	{ time hadoop jar ${jarName} ${hadoopDriver} ${hdfsRoot}/${hdfsInput} ${hdfsRoot}/${hdfsOutput} mean;} 2> time/time_pseudo_mean.txt
	mkdir -p ${localOutput}
	mkdir -p ${localOutput}/pseudo_mean
	hadoop fs -get ${hdfsRoot}/${hdfsOutput}/* ${localOutput}/pseudo_mean

pseudo-median:
	mkdir -p time
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${hadoopDriver}.java
	jar cf ${jarName} *.class	
	hadoop fs -rm -r -f ${hdfsOutput}
	{ time hadoop jar ${jarName} ${hadoopDriver} ${hdfsRoot}/${hdfsInput} ${hdfsRoot}/${hdfsOutput} median ;} 2> time/time_pseudo_median.txt
	mkdir -p ${localOutput}
	mkdir -p ${localOutput}/pseudo_median
	hadoop fs -get ${hdfsRoot}/${hdfsOutput}/* ${localOutput}/pseudo_median

pseudo-fast:
	mkdir -p time
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${hadoopDriver}.java
	jar cf ${jarName} *.class	
	hadoop fs -rm -r -f ${hdfsOutput}
	{ time hadoop jar ${jarName} ${hadoopDriver} ${hdfsRoot}/${hdfsInput} ${hdfsRoot}/${hdfsOutput} fast ;} 2> time/time_pseudo_fast.txt
	mkdir -p ${localOutput}
	mkdir -p ${localOutput}/pseudo_fast
	hadoop fs -get ${hdfsRoot}/${hdfsOutput}/* ${localOutput}/pseudo_fast

cloud-mean:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${hadoopDriver}.java
	jar cf ${jarName} *.class
	aws s3 cp ${jarName} s3://${awsBucketName}
	aws s3 rm s3://${awsBucketName}/${awsOutputDir} --recursive
	aws emr create-cluster --name "CLI-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=${awsInstance} InstanceGroupType=CORE,InstanceCount=2,InstanceType=${awsInstance} --steps Type=CUSTOM_JAR,Name="CusJarStep",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${jarName},Args=[${hadoopDriver},s3n://${awsBucketName}/${awsInputDir}/,s3n://${awsBucketName}/${awsOutputDir}/,mean] --auto-terminate --log-uri s3://${awsBucketName}/${awsLogDir} --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=${awsRegion} --enable-debugging > clusterId.txt
	sleep 15;
	sh clusterCompleteCheck.sh `cat clusterId.txt` ${awsBucketName} mean
	mkdir -p ${localOutput}
	mkdir -p ${localOutput}/cloud_mean
	aws s3 sync s3://${awsBucketName}/${awsOutputDir}/* ${localOutput}/cloud_mean

cloud-median:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${hadoopDriver}.java
	jar cf ${jarName} *.class
	aws s3 cp ${jarName} s3://${awsBucketName}
	aws s3 rm s3://${awsBucketName}/${awsOutputDir} --recursive
	aws emr create-cluster --name "CLI-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=${awsInstance} InstanceGroupType=CORE,InstanceCount=2,InstanceType=${awsInstance} --steps Type=CUSTOM_JAR,Name="CusJarStep",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${jarName},Args=[${hadoopDriver},s3n://${awsBucketName}/${awsInputDir}/,s3n://${awsBucketName}/${awsOutputDir}/,median] --auto-terminate --log-uri s3://${awsBucketName}/${awsLogDir} --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=${awsRegion} --enable-debugging > clusterId.txt
	sleep 5;
	sh clusterCompleteCheck.sh `cat clusterId.txt` ${awsBucketName} median
	mkdir -p ${localOutput}
	mkdir -p ${localOutput}/cloud_median
	aws s3 sync s3://${awsBucketName}/${awsOutputDir}/* ${localOutput}/cloud_median

cloud-fast:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${hadoopDriver}.java
	jar cf ${jarName} *.class
	aws s3 cp ${jarName} s3://${awsBucketName}
	aws s3 rm s3://${awsBucketName}/${awsOutputDir} --recursive
	aws emr create-cluster --name "CLI-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=${awsInstance} InstanceGroupType=CORE,InstanceCount=2,InstanceType=${awsInstance} --steps Type=CUSTOM_JAR,Name="CusJarStep",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${jarName},Args=[${hadoopDriver},s3n://${awsBucketName}/${awsInputDir}/,s3n://${awsBucketName}/${awsOutputDir}/,fast] --auto-terminate --log-uri s3://${awsBucketName}/${awsLogDir} --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=${awsRegion} --enable-debugging > clusterId.txt
	sleep 5;
	sh clusterCompleteCheck.sh `cat clusterId.txt` ${awsBucketName} fast
	mkdir -p ${localOutput}
	mkdir -p ${localOutput}/cloud_fast
	aws s3 sync s3://${awsBucketName}/${awsOutputDir}/* ${localOutput}/cloud_fast

all:
	single-mean single-median multi-mean multi-median pseudo-mean pseudo-median pseudo-fast cloud-mean cloud-median cloud-fast
	Rscript -e "rmarkdown::render('Assignment3_Report.Rmd')"

report:
	Rscript -e "rmarkdown::render('Assignment3_Report.Rmd')"

unsafe:
	hdfs dfsadmin -safemode leave