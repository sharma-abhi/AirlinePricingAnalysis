#!bin/bash
# Makefile for Map Reduce A4 project.
# Sharma, Abhijeet and Khan, Afan Ahmad

# CONFIGURATION PARAMETERS
localInputPath=/home/abhijeet/Sharma_Khan_A5
localInputDir=tiny
localOutputPath=/home/abhijeet/Sharma_Khan_A5
localOutputDir=output
firstJarName=cc.jar
firstHadoopDriver=FlightCount

# Pseudo-Distributed Hadoop
hadoopVersion=2.6.3
hdfsRootPath=/user/abhijeet
hdfsInputDir=tiny
hdfsOutputDir=output


# Cloud
awsRegion=us-east-1a
awsInstance=m1.medium 
awsBucketName=bucketforabhia3
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
	hadoop fs -mkdir -p ${hdfsRootPath}/${hdfsInputDir}

hadoop-upload:
	hadoop fs -put ${localInputPath}/${localInputDir}/*.gz ${hdfsRootPath}/${hdfsInputDir}

cloud-setup:
	aws s3 mb s3://${awsBucketName}

cloud-upload:
	aws s3 sync ${localInputPath}/${localInputDir} s3://${awsBucketName}/${awsInputDir}

cloud-fetch:
	aws s3 sync s3://${awsBucketName}/${awsOutputDir} ${localOutputPath}

unsafe:
	hdfs dfsadmin -safemode leave

pseudo:
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${firstHadoopDriver}.java
	jar cf ${firstJarName} *.class
	hadoop fs -rm -r -f ${hdfsRootPath}/${hdfsOutputDir}
	hadoop jar ${firstJarName} ${firstHadoopDriver} ${hdfsRootPath}/${hdfsInputDir} ${hdfsRootPath}/${hdfsOutputDir}
	mkdir -p ${localOutputPath}/${localOutputDir}
	hadoop fs -get ${hdfsRootPath}/${hdfsOutputDir}/* ${localOutputPath}/${localOutputDir}
	#Rscript -e "rmarkdown::render('Assignment5_Report.Rmd')" ${localOutputPath}/${localOutputDir}

cloud:
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${firstHadoopDriver}.java
	jar cf ${firstJarName} *.class
	aws s3 rm s3://${awsBucketName}/${awsOutputDir} --recursive
	aws s3 cp ${firstJarName} s3://${awsBucketName}
	aws emr create-cluster --name "CLI-Cluster1" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=${awsInstance} InstanceGroupType=CORE,InstanceCount=2,InstanceType=${awsInstance} --steps Type=CUSTOM_JAR,Name="MapReduce1",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${firstJarName},Args=[${firstHadoopDriver},s3n://mrclassvitek/data/,s3n://${awsBucketName}/${awsOutputDir}/] --auto-terminate --log-uri s3://${awsBucketName}/${awsLogDir} --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=${awsRegion} --enable-debugging > clusterId.txt
	sleep 5;
	sh clusterCompleteCheck.sh `cat clusterId.txt`
	mkdir -p ${localOutputPath}/${localOutputDir}
	mkdir -p ${localOutputPath}/${localOutputDir}/mr1_output
	aws s3 sync s3n://${awsBucketName}/${awsOutputDir}/ ${localOutputPath}/${localOutputDir}/mr1_output
	Rscript 'linearReg.R' ${localOutputPath}/${localOutputDir}/mr1_output ${nValue}
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${secondHadoopDriver}.java
	jar cf ${secondJarName} *.class
	aws s3 rm s3://${awsBucketName}/${awsOutputDir} --recursive
	aws s3 cp ${secondJarName} s3://${awsBucketName}
	aws emr create-cluster --name "CLI-Cluster2" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=${awsInstance} InstanceGroupType=CORE,InstanceCount=2,InstanceType=${awsInstance} --steps Type=CUSTOM_JAR,Name="MapReduce2",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${secondJarName},Args=[${secondHadoopDriver},s3n://mrclassvitek/data/,s3n://${awsBucketName}/${awsOutputDir}/,$(shell cat best_airline.txt )] --auto-terminate --log-uri s3://${awsBucketName}/${awsLogDir} --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=${awsRegion} --enable-debugging > clusterId2.txt
	sleep 5;
	sh clusterCompleteCheck.sh `cat clusterId2.txt`
	mkdir -p ${localOutputPath}/${localOutputDir}/mr2_output
	aws s3 sync s3://${awsBucketName}/${awsOutputDir}/ ${localOutputPath}/${localOutputDir}/mr2_output/
	Rscript -e "rmarkdown::render('Assignment4_Report.Rmd')" ${localOutputPath}/${localOutputDir}/mr2_output `cat best_airline.txt` ${nValue}