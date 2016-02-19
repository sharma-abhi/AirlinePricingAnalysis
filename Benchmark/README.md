Author: Abhijeet Sharma and Afan Ahmad Khan
Date created: February 6, 2016

DESCRIPTION
The program takes a hadoop directory path as input containing multiple gzipped csv files and creates
a plot displaying the average ticket prices of all airlines per month(restricted for airlines active 
in 2015).
The program also benchmarks and compares the cost of computing (A) mean and (B) median price, and 
(C) fast median for (i) single threaded Java, (ii) multi-threaded Java, (iii) pseudo-distributed MR, 
and (iv) distributed MR.
The program evaluates the performance of the following configurations i-A, i-B, ii-A, ii-B, iii-A, iii-B, iii-C, 
iv-A, iv-B, iv-C.

1. LIST OF FILES PROVIDED:
	1.1  README.md (Description of Submission in Markdown Format)
	1.2  Makefile (Makefile for the project)
	1.3  Assignment3_Report.pdf (Report for the project in PDF)
	1.4  Assignment3_Report.Rmd (Rmd file for the Report)
	1.5  Solution.java (Java file for Single/MultiThreading configuration)
	1.6  FileThread.java (Helper file for Solution.java)
	1.7  FlightInfo.java (Helper file for Solution.java)
	1.8  javacsv.jar (Helper file for Solution.java for reading csv files)
	1.9  CarrierCount.java (Java file for Hadoop-Pseudo/AWS configuration)
	1.10 clusterCompleteCheck.sh (Shell script for checking whether job is complete in cluster)
	1.11 time_benchmark.csv (CSV file comparing time for different configurations)
	1.12 cc.jar (JAR file for .java files)
	
2. SYSTEM REQUIREMENTS:
	2.1. Java 1.7.0_79
	2.2. Ubuntu 14.04 64-bit
	2.3. 8GB RAM
	2.4. Pandoc (https://github.com/jgm/pandoc/releases/tag/1.16.0.2)
	2.4. R Packages:
	  2.4.1 Rcpp
	  2.4.2 R.utils
	  2.4.3 ggplot2
	  2.4.4 rmarkdown
	  2.4.5 plyr

3. STEPS TO RUN (Note: Make sure all files/subfolders are present in the same folder before running):
	NOTE: Please add the required input files in the "input" folder of hadoop(for pseudo) or "<BUCKETNAME>/input"(for cloud) before execution.
	NOTE2: Change the Configuration Parameters(found in the top) in the MakeFile before running any jobs.
	
	For Running Single Threading:
		3.1.1 make single-mean
		3.1.2 make single-median
	For Running Multi Threading:
		3.2.1 make multi-mean
		3.2.2 make multi-median
	For Formatting NameNode:
		3.3 make format
	For Setting up HDFS/S3 directory and Upload input data:
		3.4 hadoop-setup
		3.5 hadoop-upload
		3.6 cloud-setup
		3.7 cloud-upload
	For Starting hadoop server on local system:
		3.8 make hstart
	For Stopping hadoop server on local system:
		3.9 make hstop
	For Running Pseudo-Distributed hadoop on local system:
		3.10.1 make pseudo-mean
		3.10.2 make pseudo-median
		3.10.3 make pseudo-fast
	For Running AWS Distributed hadoop on cloud:
    	3.11.1 make cloud-mean
    	3.11.2 make cloud-median
    	3.11.3 make cloud-fast
    For Generating Report:
    	3.12 make report
    For Running All configurations:
    	WARNING: Need to complete 3.1, 3.2, 3.3, 3.4 and 3.5 first.
    	3.13 make all
    For Running leaving hdfs safe mode(Optional):
    	3.14 make unsafe

4. LIST OF FOLDERS(Generated only after run of repective programs):
	4.1 output (Folder for Single/Multi-Threaded/Hadoop-Pseudo/AWS EMR configurations output data)
	4.2 time (Folder for files containing time elapsed for Single/Multi-Threaded/Hadoop-Pseudo jobs)
	4.3 logs (Folder for files containing time elapsed for AWS EMR jobs)