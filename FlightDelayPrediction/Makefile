#!bin/bash
# Makefile for Map Reduce A4 project.
# Sharma, Abhijeet and Khan, Afan Ahmad

# CONFIGURATION PARAMETERS
localInputPath=/home/abhijeet/Sharma_Khan_A6
localMainInputDir=input
localOutputPath=/home/abhijeet/Sharma_Khan_A6
localOutputDir=output
firstJarName=fc.jar
firstHadoopDriver=FlightCount

# Pseudo-Distributed Hadoop
hadoopVersion=2.6.3
hdfsRootPath=/user/abhijeet

hdfsMainInputDir=input
hdfsTrainInputDir=a6history
hdfsTestInputDir=a6test

hdfsMainOutputDir=output
hdfsTrainOutputDir=a6history
hdfsTestOutputDir=a6test


# Cloud
awsRegion=us-east-1a
awsInstance=c1.medium
awsBucketName=datafora2
awsInputDir=input
awsOutputDir=output
awsLogDir=logs

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
	hadoop fs -mkdir -p ${hdfsRootPath}	

hadoop-upload:
	hadoop fs -put ${localInputPath}/${localMainInputDir} ${hdfsRootPath}/${hdfsMainInputDir}

cloud-setup:
	aws s3 mb s3://${awsBucketName}

cloud-upload:
	aws s3 sync ${localInputPath}/${localInputDir} s3://${awsBucketName}/${awsInputDir}

unsafe:
	hdfs dfsadmin -safemode leave

pseudo-train:
	rm -rf ${localOutputPath}/${localOutputDir}/${hdfsTrainOutputDir}
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${firstHadoopDriver}.java
	jar cf ${firstJarName} *.class
	hadoop fs -rm -r -f ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir}
	hadoop jar ${firstJarName} ${firstHadoopDriver} ${hdfsRootPath}/${hdfsMainInputDir}/${hdfsTrainInputDir} ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir} train
	mkdir -p ${localOutputPath}/${localOutputDir}
	hadoop fs -get ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir} ${localOutputPath}/${localOutputDir}
	# Rscript -e "rmarkdown::render('Assignment5_Report.Rmd')" ${localOutputPath}/${localOutputDir}

pseudo-test:
	rm -rf ${localOutputPath}/${localOutputDir}/${hdfsTestOutputDir}
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${firstHadoopDriver}.java
	jar cf ${firstJarName} *.class
	hadoop fs -rm -r -f ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir}
	hadoop jar ${firstJarName} ${firstHadoopDriver} ${hdfsRootPath}/${hdfsMainInputDir}/${hdfsTestInputDir} ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir} test
	mkdir -p ${localOutputPath}/${localOutputDir}
	hadoop fs -get ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir} ${localOutputPath}/${localOutputDir}

fetch-train:
	hadoop fs -get ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir} ${localOutputPath}/${localOutputDir}

cloud:
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${firstHadoopDriver}.java
	jar cf ${firstJarName} *.class
	aws s3 rm s3://${awsBucketName}/${awsOutputDir} --recursive
	aws s3 cp ${firstJarName} s3://${awsBucketName}
	aws emr create-cluster --name "CLI-Cluster1" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=${awsInstance} InstanceGroupType=CORE,InstanceCount=10,InstanceType=${awsInstance} --steps Type=CUSTOM_JAR,Name="MapReduce",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${firstJarName},Args=[${firstHadoopDriver},s3n://${awsBucketName}/${awsInputDir}/,s3n://${awsBucketName}/${awsOutputDir}/] --auto-terminate --log-uri s3://${awsBucketName}/${awsLogDir} --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=${awsRegion} --enable-debugging > clusterId.txt
	sleep 5;
	sh clusterCompleteCheck.sh `cat clusterId.txt`
	mkdir -p ${localOutputPath}/${localOutputDir}
	aws s3 sync s3://${awsBucketName}/${awsOutputDir}/ ${localOutputPath}/${localOutputDir}
	Rscript -e "rmarkdown::render('Assignment5_Report.Rmd')" ${localOutputPath}/${localOutputDir}

cloud-fetch:
	mkdir -p ${localOutputPath}/${localOutputDir}
	aws s3 sync s3://${awsBucketName}/${awsOutputDir}/ ${localOutputPath}/${localOutputDir}

report:
	Rscript -e "rmarkdown::render('Assignment5_Report.Rmd')" ${localOutputPath}/${localOutputDir}
