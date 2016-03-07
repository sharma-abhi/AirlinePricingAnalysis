#!bin/bash
# Makefile for Map Reduce A6 project.
# Sharma, Abhijeet and Khan, Afan Ahmad
# Please setup the configuration parameters according to your system properties.

############### START CONFIGURATION ##################
# IMPORTANT: SEE README.txt BEFORE RUNNING OR CHANGING PARAMETERS!!

# LOCAL CONFIGURATION PARAMETERS 
## Input
localInputPath=/home/abhijeet/Sharma_Khan_A6
localMainInputDir=input
localTrainInputDir=a6history
localTestInputDir=a6test
localValidationInputDir=a6validate
## Output
localOutputPath=/home/abhijeet/Sharma_Khan_A6
localMainOutputDir=output

# PSEUDO-DISTRIBUTED HADOOP (only for Processing Input Files)
## Jar
HadoopJarName=pp.jar
HadoopDriver=PreProcessFlight
## Hadoop
hadoopVersion=2.6.3
hdfsRootPath=/user/abhijeet
## HDFS Input (for Preprocessing)
hdfsMainInputDir=input
hdfsTrainInputDir=a6history
hdfsTestInputDir=a6test
## HDFS Output (for Preprocessing)
hdfsMainOutputDir=output
hdfsTrainOutputDir=a6history
hdfsTestOutputDir=a6test

# SCALA-SPARK CONFIGURATION (for Machine Learning)
## Jar
SparkJarName=sparkModel.jar
SparkDriver=SparkModel
## Spark Input
preProcessedTrainFile=${localOutputPath}/${localMainOutputDir}/${localTrainInputDir}/final-clean-train-data
preProcessedTestFile=${localOutputPath}/${localMainOutputDir}/${localTestInputDir}/final-clean-test-data
validationFile=${localInputPath}/${localMainInputDir}/${localValidationInputDir}/98validate.csv.gz
## Spark Output
FeatureImportancesFile=${localOutputPath}/${localMainOutputDir}/FeatureImportances
TestPredictionsFile=${localOutputPath}/${localMainOutputDir}/TestPredictions
ModelMetricsFile=${localOutputPath}/${localMainOutputDir}/ModelMetrics

# CLOUD AWS CONFIGURATION
awsRegion=us-east-1a
awsInstanceType=m3.xlarge
awsInstanceCount=3
awsBucketName=bucketforabhia3
awsMainOutputDir=output
awsLogDir=logs
## AWS Input (for Preprocessing)
awsInputTrainFilePath=s3n://mrclassvitek/a6history
awsInputTestFilePath=s3n://mrclassvitek/a6test
awsInputValidationFilePath=s3n://mrclassvitek/a6validate
## AWS Output (for Preprocessing)
awsPreProcessOutputTrainFilePath=s3://${awsBucketName}/${awsMainOutputDir}/${hdfsTrainOutputDir}
awsPreProcessOutputTestFilePath=s3://${awsBucketName}/${awsMainOutputDir}/${hdfsTestOutputDir}
## AWS Spark Input (for Machine Learning)
awsSparkInputTrainFile=s3://${awsBucketName}/cleanInput/final-clean-train-data
awsSparkInputTestFile=s3://${awsBucketName}/cleanInput/final-clean-test-data
awsSparkInputValidationFile=${awsInputValidationFilePath}/98validate.csv.gz
## AWS Spark Output (for Machine Learning)
awsSparkOutputFeatureImportancesFile=s3://${awsBucketName}/${awsMainOutputDir}/FeatureImportances
awsSparkOutputTestPredictionsFile=s3://${awsBucketName}/${awsMainOutputDir}/TestPredictions
awsSparkOutputModelMetricsFile=s3://${awsBucketName}/${awsMainOutputDir}/ModelMetrics
############### END CONFIGURATION ##################

# UTILITY
## After setting up hadoop for first time
format: 
	hdfs namenode -format
## Leave safe-mode if Hadoop gets stuck in safemode 
unsafe:
	hdfs dfsadmin -safemode leave

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
	rm -rf ${localOutputPath}/${localMainOutputDir}/${hdfsTrainOutputDir}
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar: ${HadoopDriver}.java
	jar cf ${HadoopJarName} *.class
	hadoop fs -rm -r -f ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir}
	hadoop jar ${HadoopJarName} ${HadoopDriver} ${hdfsRootPath}/${hdfsMainInputDir}/${hdfsTrainInputDir} ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir} train
	mkdir -p ${localOutputPath}/${localMainOutputDir}
	hadoop fs -get ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTrainOutputDir} ${localOutputPath}/${localMainOutputDir}
	cat ${localOutputPath}/${localMainOutputDir}/${hdfsTrainOutputDir}/part-r* > ${localOutputPath}/${localMainOutputDir}/${localTrainInputDir}/final-clean-train-data
## preprocessing test files
hadoop-preprocess-test:
	rm -rf ${localOutputPath}/${localMainOutputDir}/${hdfsTestOutputDir}
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar: ${HadoopDriver}.java
	jar cf ${HadoopJarName} *.class
	hadoop fs -rm -r -f ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir}
	hadoop jar ${HadoopJarName} ${HadoopDriver} ${hdfsRootPath}/${hdfsMainInputDir}/${hdfsTestInputDir} ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir} test
	mkdir -p ${localOutputPath}/${localMainOutputDir}
	hadoop fs -get ${hdfsRootPath}/${hdfsMainOutputDir}/${hdfsTestOutputDir} ${localOutputPath}/${localMainOutputDir}
	cat ${localOutputPath}/${localMainOutputDir}/${hdfsTestOutputDir}/part-r* > ${localOutputPath}/${localMainOutputDir}/${localTestInputDir}/final-clean-test-data
## preprocessing Input files
hadoop-preprocess:	hadoop-preprocess-train hadoop-preprocess-test	
## running machine learning in Spark (local)
spark-ml:
	cd SparkModel && sbt clean package
	cd SparkModel && mv target/scala*/*.jar ${SparkJarName}
	cd SparkModel && spark-submit --master local[*] --driver-memory 5g --executor-memory 1g ${SparkJarName} ${preProcessedTrainFile} ${preProcessedTestFile} ${validationFile} ${FeatureImportancesFile} ${TestPredictionsFile} ${ModelMetricsFile}
## Generate Report
report:
	Rscript -e "rmarkdown::render('Assignment6_Report.Rmd')" ${localOutputPath}/${localMainOutputDir}\
## Run Whole Pipeline in Local Machine (make sure hadoop-start, hadoop-setup and hadoop-upload is already executed)
run-pipeline-local:hadoop-preprocess spark-ml 

# RUNNING PIPELINE IN AWS SERVER
## Setup S3 bucket
cloud-setup:
	aws s3 mb s3://${awsBucketName}
## Upload Spark Configuration JSON file to S3 bucket (MAKE SURE TO GRANT ALL PERMISSIONS TO EVERYONE FOR JSON FILE IN S3 !!)
cloud-preprocess:
	javac -cp commons-lang3-3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-${hadoopVersion}.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoopVersion}.jar:  ${HadoopDriver}.java
	jar cf ${HadoopJarName} *.class
	aws s3 rm s3://${awsBucketName}/${awsMainOutputDir} --recursive
	aws s3 rm s3://${awsBucketName}/cleanInput --recursive
	aws s3 cp ${HadoopJarName} s3://${awsBucketName}
	aws s3 cp sparkConfig.json s3://${awsBucketName} --grants full=uri=http://acs.amazonaws.com/groups/global/AllUsers
	aws emr create-cluster --name "Flight Delay Prediction Cluster" --release-label emr-4.3.0 --applications Name=Hadoop Name=Spark --configurations https://s3.amazonaws.com/${awsBucketName}/sparkConfig.json --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=${awsInstanceType} InstanceGroupType=CORE,InstanceCount=${awsInstanceCount},InstanceType=${awsInstanceType} --steps Type=CUSTOM_JAR,Name="PreProcessTrainData",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${HadoopJarName},Args=[${HadoopDriver},${awsInputTrainFilePath},${awsPreProcessOutputTrainFilePath},train] --log-uri s3://${awsBucketName}/${awsLogDir} --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=${awsRegion} --enable-debugging > clusterId.txt
	aws emr add-steps --cluster-id `cat clusterId.txt` --steps Type=CUSTOM_JAR,Name="PreProcessTestData",ActionOnFailure=CONTINUE,Jar=s3://${awsBucketName}/${HadoopJarName},Args=[${HadoopDriver},${awsInputTestFilePath},${awsPreProcessOutputTestFilePath},test]
	sleep 20;
	sh clusterWaitingCheck.sh `cat clusterId.txt`
	rm -rf ${localOutputPath}/${localMainOutputDir}
	mkdir -p ${localOutputPath}/${localMainOutputDir}
	aws s3 cp ${awsPreProcessOutputTrainFilePath} ${localOutputPath}/${localMainOutputDir}/${hdfsTrainOutputDir} --recursive
	cat ${localOutputPath}/${localMainOutputDir}/${hdfsTrainOutputDir}/part-r-* > ${localOutputPath}/${localMainOutputDir}/${hdfsTrainOutputDir}/final-clean-train-data
	aws s3 cp ${localOutputPath}/${localMainOutputDir}/${hdfsTrainOutputDir}/final-clean-train-data s3://${awsBucketName}/cleanInput/
	aws s3 cp ${awsPreProcessOutputTestFilePath} ${localOutputPath}/${localMainOutputDir}/${hdfsTestOutputDir} --recursive
	cat ${localOutputPath}/${localMainOutputDir}/${hdfsTestOutputDir}/part-r-* > ${localOutputPath}/${localMainOutputDir}/${hdfsTestOutputDir}/final-clean-test-data
	aws s3 cp ${localOutputPath}/${localMainOutputDir}/${hdfsTestOutputDir}/final-clean-test-data s3://${awsBucketName}/cleanInput/
	cd SparkModel && sbt clean package
	cd SparkModel && mv target/scala*/*.jar ${SparkJarName}
	cd SparkModel && aws s3 cp ${SparkJarName} s3://${awsBucketName}
	aws emr add-steps --cluster-id `cat clusterId.txt` --steps Type=Spark,Name="Spark Model",ActionOnFailure=CONTINUE,Args=[--master,yarn-cluster,--deploy-mode,cluster,--class,${SparkDriver},s3n://${awsBucketName}/${SparkJarName},${awsSparkInputTrainFile},${awsSparkInputTestFile},${awsSparkInputValidationFile},${awsSparkOutputFeatureImportancesFile},${awsSparkOutputTestPredictionsFile},${awsSparkOutputModelMetricsFile}]
	sleep 20;
	sh clusterWaitingCheck.sh `cat clusterId.txt`
	aws s3 cp ${awsSparkOutputFeatureImportancesFile} ${FeatureImportancesFile} --recursive
	aws s3 cp ${awsSparkOutputTestPredictionsFile} ${TestPredictionsFile} --recursive
	aws s3 cp ${awsSparkOutputModelMetricsFile} ${ModelMetricsFile} --recursive
	aws emr terminate-clusters --cluster-ids `cat clusterId.txt`
	cat ${TestPredictionsFile}/part-* > ${TestPredictionsFile}/final-predictions
## Run Whole Pipeline in AWS EMR (make sure cloud-setup is already executed and "Default output format" on running "aws configure" is set as "text")
run-pipeline-cloud: cloud-preprocess cloud-spark