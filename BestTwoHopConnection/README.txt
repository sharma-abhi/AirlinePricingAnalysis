Author: Abhijeet Sharma, Deepen Mehta, Akshay Raje and Afan Ahmad Khan
Email: sharma.abhi@husky.neu.edu
Phone: (617)373-0355
Date created: March 20, 2016

DESCRIPTION
The program builds a Flight Prediction model which trains on history data and predicts on test dataset.

1. LIST OF FILES and FOLDERS PROVIDED:	
	1.  A Sharma_Mehta_Raje_Khan.gz file. Unzip this folder. The folder unpacks it to a Sharma_Mehta_Raje_Khan folder. This folder contains the following files
	2   README.txt 
	3.  Makefile 
	4.  Assignment7_Report.pdf 
	5.  Assignment7_Report.Rmd 
	6.  PreProcessFlight.java 
    7.  PreProcessMapper.java 
    8.  PreProcessReducer.java 
    9.  CSVParser.java, CSVReaderNullFieldIndicator.java, commons-lang3-3.4.jar - 
    10. clusterWaitingCheck.sh 
	11. SparkModel/SparkModel.scala 
	12. SparkModel/build.sbt 
	
2. DIRECTORY STRUCTURE:

    * has to be set by user manually before run
    + is created by the Program during execution

    |Sharma_Mehta_Raje_Khan
        |README.txt                         (Description of the Submission)
        |MakeFile                           (Makefile for the project)
        |Assignment7_Report.pdf             (PDF Report of the project results)
        |Assignment7_Report.Rmd             (Rmd file for the Report)
        |PreProcessFlight.java              (Hadoop MR Driver Java file)
        |PreProcessMapper.java              (Hadoop MR Mapper Java file)
        |PreProcessMapper.java              (Hadoop MR Driver Java file)
        |CSVParser.java                     (Open Source Implementation For parsing of input records)
        |CSVReaderNullFieldIndicator.java   (Helper File for CSVParser.java)
        |commons-lang3-3.4.jar              (Helper File for CSVParser.java)
        |clusterWaitingCheck.sh             (Shell script which waits for cluster to complete a step)
        |sparkConfig.json                   (Configuration File for Spark in JSON)
        |SparkModel
            |SparkModel.scala               (Spark Machine Learning Model Creation Scala file)
            |build.sbt                      (sbt File for building scala JAR package)
        |input*
            |a7history|(36 csv.gz files)*
            |a7test|04redacted.csv.gz*
            |a7validate|04missed.csv.gz*
            |a7request|04req10k.csv.gz*
        |output+
            |a7history+
            |a7test+
            |TestPredictions
            |FeatureImportances

3. SYSTEM SPECIFICATION & REQUIREMENTS:
    1. Ubuntu 14.04 64-bit, 8GB RAM
    2. Java 1.7.0_79
    3. Apache Hadoop v.2.6.3 (http://www-us.apache.org/dist/hadoop/common/hadoop-2.6.3/hadoop-2.6.3.tar.gz)
    4. Scala v.2.10.6 (http://www.scala-lang.org/download/2.10.6.html)
    4. sbt 
      Linux: http://www.scala-sbt.org/release/docs/Installing-sbt-on-Linux.html
      Mac: http://www.scala-sbt.org/release/docs/Installing-sbt-on-Mac.html
    5. Apache Spark v.1.6.0 (http://www-eu.apache.org/dist/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz)
    6. Pandoc (https://github.com/jgm/pandoc/releases/tag/1.16.0.2)
    7. R Packages:
    7.1 ggplot2
    7.2 rmarkdown
    7.3 e1071
    7.4 caret
    7.5 plyr
    7.6 R.utils

4. STEPS TO RUN (Note: Make sure all files/subfolders are present as mentioned in #2 before running):
    NOTE: We assume the user has install Java, Hadoop, Scala and Spark before running the jobs.  See #3 for package versions.
	NOTE2: Change the Configuration Parameters(found in the top) of the MakeFile before running any jobs.
	
    4.1 Type "aws configure" and make sure "Default output format" is text.  --> VERY IMPORTANT STEP
    4.2 Navigate to the Sharma_Mehta_Raje_Khan folder.
    4.3 Make necessary changes in the configuration parameters of the MakeFile.
		4.3.1 localInputPath          -> Input Directory Path (Eg: /home/abhijeet/Sharma_Mehta_Raje_Khan)
        4.3.2 localMainInputDir       ->  Local Main Input Directory Name (Eg: input)
        4.3.3 localTrainInputDir      ->  Local Input Train Directory Name (Eg: a7history)
        4.3.4 localTestInputDir       ->  Local Input Test Directory Name (Eg: a7test)
        4.3.5 localValidationInputDir ->  LocalInput Validation Directory Name (Eg: a7validate)
        4.3.6 localOutputPath         -> Local Output Directory Path (Eg: /home/abhijeet/Sharma_Mehta_Raje_Khan)
        4.3.7 localMainOutputDir      -> Main Output Directory Name (Eg: output)
        4.3.8 hadoopVersion           -> Hadoop Version Number (Eg: 2.6.3)
        4.3.9 hdfsRootPath            -> Root Path of HDFS (Eg: /user/abhijeet)
        4.3.10 hdfsMainInputDir       -> HDFS Main Input Directory Name (Eg: input)
        4.3.11 hdfsTrainInputDir      -> HDFS Input Train Directory Name (Eg: a7history)
        4.3.12 hdfsTestInputDir       -> HDFS Input Test Directory Name (Eg: a7test)
        4.3.13 hdfsMainOutputDir      -> HDFS Main Output Directory Name (Eg: output)
        4.3.14 hdfsTrainOutputDir     -> HDFS Output Train Directory Name (Eg: a7history)
        4.3.15 hdfsTestOutputDir      -> HDFS Output Test Directory Name (Eg: a7test)
        4.3.16 awsRegion
        4.3.17 awsInstanceType
        4.3.18 awsInstanceCount
        4.3.19 awsBucketName
        4.3.20 awsMainOutputDir
	
    4.4 For Running in Local Machine, Pipeline = Pseudo MR (Preprocessing)  + Local Spark (Machine Learning)
        PRE-REQUISITES CHECK:
            Typing "hadoop version", scala -version", "sbt help" and "spark-submit --version" should work.If not, it means respective software has not been installed properly.
        4.4.1 make hadoop-start           // starts hadoop
        4.4.2 make hadoop-setup           // creates HDFS root directory(/user/abhijeet)
        4.4.3 make hadoop-upload          // uploads input files to HDFS
		4.4.4 make run-pipeline-local     // runs the Local Pipeline

	4.5 For Running in AWS Cloud:
        NOTE: Please make sure you create the Bucket in the same zone as the EMR machine.Otherwise, it'll fail
        4.5.1 make cloud-setup            // create Bucket
		4.5.2 make run-pipeline-cloud     // runs the AWS Pipeline		

5. LIST OF FILES AND FOLDERS GENERATED: (only after run of respective programs):
	5.1 For Local Machine:
		5.1.1 Assignment7_Report.pdf                    // The pdf report generated for the program
        5.1.2 output/a7history/final-clean-train-data   // The Sanitized Training Dataset
        5.1.3 output/a7test/final-clean-test-data       // The Sanitized Testing Dataset
        5.1.4 output/FeatureImportances/part-00000      // File containing importances of each feature generated by Spark
        5.1.5 output/TestPredictions/final-predictions  // Consolidated file containing predictions of all test data
	5.2 For Cloud:
        5.2.1 All files as generated for Local Machine pipeline.
		5.2.3 clusterId.txt - Text File containing the id of the cluster created for the Pipeline
