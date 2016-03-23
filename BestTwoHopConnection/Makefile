#!bin/bash
# Makefile for Map Reduce A6 project.
# Sharma, Abhijeet; Khan, Afan Ahmad; Mehta, Deepen; Raje, Akshay
# Please setup the configuration parameters according to your system properties.

############### START CONFIGURATION ##################
# IMPORTANT: SEE README.txt BEFORE RUNNING OR CHANGING PARAMETERS!!

# LOCAL CONFIGURATION PARAMETERS 
## Input
localInputPath=/home/abhijeet/Sharma_Mehta_Raje_Khan_A7
localMainInputDir=input
localTrainInputDir=a7history
localTestInputDir=a7test
localValidationInputDir=a7validate
## Output
localOutputPath=/home/abhijeet/Sharma_Mehta_Raje_Khan_A7
localMainOutputDir=output
############### LOCAL ##################
# PSEUDO-DISTRIBUTED HADOOP (only for Processing Input Files)
## Jar
HadoopJarName=pp.jar
HadoopDriver=PreProcessFlight
## Hadoop
hadoopVersion=2.6.3
hdfsRootPath=/user/abhijeet
## HDFS Input (for Preprocessing)
hdfsMainInputDir=input
hdfsTrainInputDir=a7history
hdfsTestInputDir=a7test
hdfsRequestInputDir=a7request
## HDFS Output (for Preprocessing)
hdfsMainOutputDir=output
hdfsTrainOutputDir=a7history
hdfsTestOutputDir=a7test

# SCALA-SPARK CONFIGURATION (for Machine Learning)
## Jar
SparkJarName=sparkModel.jar
SparkDriver=SparkModel
## Spark Input
preProcessedTrainFile=${localOutputPath}/${localMainOutputDir}/${localTrainInputDir}/final-clean-train-data
preProcessedTestFile=${localOutputPath}/${localMainOutputDir}/${localTestInputDir}/final-clean-test-data
validationFile=${localInputPath}/${localMainInputDir}/${localValidationInputDir}/04missed.csv.gz
## Spark Output
FeatureImportancesFile=${localOutputPath}/${localMainOutputDir}/FeatureImportances
TestPredictionsFile=${localOutputPath}/${localMainOutputDir}/TestPredictions

############### CLOUD AWS CONFIGURATION ##################
awsRegion=us-east-1a
awsInstanceType=m3.xlarge
awsInstanceCount=3
awsBucketName=bucketforabhia3
awsMainOutputDir=output
awsLogDir=logs
## AWS Input (for Preprocessing)
awsInputTrainFilePath=s3n://mrclassvitek/a7history
awsInputTestFilePath=s3n://mrclassvitek/a7test
awsInputRequestFilePath=s3n://mrclassvitek/07request
awsInputValidationFilePath=s3n://mrclassvitek/a7validate
## AWS Output (for Preprocessing)
awsPreProcessOutputTrainFilePath=s3://${awsBucketName}/${awsMainOutputDir}/${hdfsTrainOutputDir}
awsPreProcessOutputTestFilePath=s3://${awsBucketName}/${awsMainOutputDir}/${hdfsTestOutputDir}
## AWS Spark Input (for Machine Learning)
awsSparkInputTrainFile=s3://${awsBucketName}/${awsMainOutputDir}/${hdfsTrainOutputDir}/final-clean-train-data
awsSparkInputTestFile=s3://${awsBucketName}/${awsMainOutputDir}/${hdfsTestOutputDir}/final-clean-test-data
#awsSparkInputValidationFile=${awsInputValidationFilePath}/04missed.csv.gz
## AWS Spark Output (for Machine Learning)
awsSparkOutputFeatureImportancesFile=s3://${awsBucketName}/${awsMainOutputDir}/FeatureImportances
awsSparkOutputTestPredictionsFile=s3://${awsBucketName}/${awsMainOutputDir}/TestPredictions
############### END CONFIGURATION ##################

# UTILITY
## After setting up hadoop for first time
format: 
	hdfs namenode -format
## Leave safe-mode if Hadoop gets stuck in safemode 
unsafe:
	hdfs dfsadmin -safemode leave
## Terminate cluster	
terminate:	
	aws emr terminate-clusters --cluster-ids `cat clusterId.txt`

# RUNNING PIPELINE IN LOCAL MACHINE
## Start HDFS, YARN and History Server
hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver
## Stop HDFS, YARN and History Server
hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh
## Setup HDFS Root directory
hadoop-setup:
	hadoop fs -mkdir -p ${hdfsRootPath}	
## Upload Input Directory to HDFS Root directory
hadoop-upload:
	hadoop fs -put ${localInputPath}/${localMainInputDir} ${hdfsRootPath}/${hdfsMainInputDir}
## preprocessing train files
hadoop-preprocess-train:
	rm -rf ${localOutputPath}/${localMainOutputDir}
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar: ${HadoopDriver}.java
	jar cf ${HadoopJarName} *.class
	hadoop fs -rm -r -f ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir}
	hadoop jar ${HadoopJarName} ${HadoopDriver} ${hdfsRootPath}/${hdfsMainInputDir}/${hdfsTrainInputDir} ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir} ${hdfsRootPath}/${hdfsMainInputDir}/${hdfsRequestInputDir} train
	mkdir -p ${localOutputPath}/${localMainOutputDir}
	hadoop fs -get ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir} ${localOutputPath}/${localMainOutputDir}
## preprocessing test files
hadoop-preprocess-test:
	rm -rf ${localOutputPath}/${localMainOutputDir}/${hdfsTestOutputDir}
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar: ${HadoopDriver}.java
	jar cf ${HadoopJarName} *.class
	hadoop fs -rm -r -f ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir}
	hadoop jar ${HadoopJarName} ${HadoopDriver} ${hdfsRootPath}/${hdfsMainInputDir}/${hdfsTestInputDir} ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir} ${hdfsRootPath}/${hdfsMainInputDir}/${hdfsRequestInputDir} test
	mkdir -p ${localOutputPath}/${localMainOutputDir}
	hadoop fs -get ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir} ${localOutputPath}/${localMainOutputDir}	
## preprocessing Input files
hadoop-preprocess:	hadoop-preprocess-train hadoop-preprocess-test	
## running machine learning in Spark (local)
spark-ml:
	rm -rf ${FeatureImportancesFile}
	rm -rf ${TestPredictionsFile}
	cd SparkModel && sbt clean package
	cd SparkModel && mv target/scala*/*.jar ${SparkJarName}
	cd SparkModel && spark-submit --master local[*] --driver-memory 2g --executor-memory 5g ${SparkJarName} ${preProcessedTrainFile} ${preProcessedTestFile} ${FeatureImportancesFile} ${TestPredictionsFile}
	cat ${TestPredictionsFile}/part-* > ${TestPredictionsFile}/final-predictions
## Generate Report
report:
## Run Whole Pipeline in Local Machine (make sure hadoop-start, hadoop-setup and hadoop-upload is already executed)
	Rscript -e "rmarkdown::render('Assignment7_Report.Rmd')" ${TestPredictionsFile} ${localInputPath}/${localMainInputDir}/${localValidationInputDir}
run-pipeline-local: hadoop-preprocess spark-ml report

# RUNNING PIPELINE IN AWS SERVER
## Setup S3 bucket
cloud-setup:
	aws s3 mb s3://${awsBucketName}
cloud:
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${HadoopDriver}.java
	jar cf ${HadoopJarName} *.class
	aws s3 rm s3://${awsBucketName}/${awsMainOutputDir} --recursive
	aws s3 cp ${HadoopJarName} s3://${awsBucketName}
	aws s3 cp sparkConfig.json s3://${awsBucketName} --grants full=uri=http://acs.amazonaws.com/groups/global/AllUsers
	aws emr create-cluster --name "Flight Missing Prediction Cluster" --release-label emr-4.4.0 --applications Name=Hadoop Name=Spark --configurations https://s3.amazonaws.com/${awsBucketName}/sparkConfig.json --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=${awsInstanceType} InstanceGroupType=CORE,InstanceCount=${awsInstanceCount},InstanceType=${awsInstanceType} --steps Type=CUSTOM_JAR,Name="PreProcessTrainData",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${HadoopJarName},Args=[${HadoopDriver},${awsInputTrainFilePath},${awsPreProcessOutputTrainFilePath},${awsInputRequestFilePath},train] --log-uri s3://${awsBucketName}/${awsLogDir} --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=${awsRegion} --enable-debugging > clusterId.txt
	aws emr add-steps --cluster-id `cat clusterId.txt` --steps Type=CUSTOM_JAR,Name="PreProcessTestData",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${HadoopJarName},Args=[${HadoopDriver},${awsInputTestFilePath},${awsPreProcessOutputTestFilePath},${awsInputRequestFilePath},test]
	sleep 20;
	sh clusterWaitingCheck.sh `cat clusterId.txt`
	cd SparkModel && sbt clean package
	cd SparkModel && mv target/scala*/*.jar ${SparkJarName}
	cd SparkModel && aws s3 cp ${SparkJarName} s3://${awsBucketName}
	aws emr add-steps --cluster-id `cat clusterId.txt` --steps Type=Spark,Name="Spark Model",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--class,${SparkDriver},s3n://${awsBucketName}/${SparkJarName},${awsSparkInputTrainFile},${awsSparkInputTestFile},${awsSparkOutputFeatureImportancesFile},${awsSparkOutputTestPredictionsFile}]
	sleep 60;
	sh clusterWaitingCheck.sh `cat clusterId.txt`
	aws s3 cp ${awsSparkOutputFeatureImportancesFile} ${FeatureImportancesFile} --recursive
	aws s3 cp ${awsSparkOutputTestPredictionsFile} ${TestPredictionsFile} --recursive
	cat ${TestPredictionsFile}/part-* > ${TestPredictionsFile}/final-predictions
	aws emr terminate-clusters --cluster-ids `cat clusterId.txt`
## Run Whole Pipeline in AWS EMR (make sure cloud-setup is already executed and "Default output format" on running "aws configure" is set as "text")
run-pipeline-cloud: cloud report
