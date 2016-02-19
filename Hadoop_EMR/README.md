Author: Abhijeet Sharma and Afan Ahmad Khan
Date created: 29 January, 2016
Average execution time: 300 seconds

DESCRIPTION
The program takes a directory path as input containing multiple gzipped csv files and 
counts number of lines which are in incorrect format or fails sanity test (K)
and number of lines which are in correct format and passes sanity test(F)
It also calculates mean and median avg_ticket_price by carrier for flights active in January 2015
and displays it in standard output in ascending order.

1. LIST OF FILES/FOLDERS PROVIDED:
	1.1 README.md (Description of Submission in Markdown Format)
	1.2 Makefile (Makefile for the project)
	1.3 CarrierCount.java
	1.4 Assignment2_Report.Rmd
	
2. SYSTEM REQUIREMENTS:
1. Java 1.7.0_79
2. Ubuntu 14.04 64-bit
3. 8GB RAM
4. Pandoc (https://github.com/jgm/pandoc/releases/tag/1.16.0.2)
4. R Packages:
  4.1 Rcpp
  4.2 ggplot2
  4.3 knitr
  4.4 plyr

3. STEPS TO RUN (Note: Make sure all files/subfolders are present in the same folder before running):
	NOTE: Please add the required files manually in the "input" folder of hadoop(for pseudo) or "<BUCKETNAME>/input"(for cloud) before execution.
	For Formatting NameNode:
		3.1 make format
	For Starting hadoop server on local system:
		3.2 make hstart
	For Stopping hadoop server on local system:
		3.3 make hstop
	For Running Pseudo-Distributed hadoop on local system:
		3.4 make pseudo
	For Running AWS Distributed hadoop on cloud:
		3.5. Enter Bucket name in MakeFile
    	3.6  make cloud