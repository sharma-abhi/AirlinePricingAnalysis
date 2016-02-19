Author: Abhijeet Sharma and Afan Ahmad Khan
Date created: February 13, 2016


DESCRIPTION
The program takes a hadoop directory path as input containing multiple gzipped csv files and creates
plots displaying the median ticket prices of the cheapest airlines per week.
The program consists of two pipelined MR jobs. The first MR job sends the relevant fields  - Carrier code,
Year, Average Price and Scheduled Time to the R script linearReg.R. This script computes the cheapest airline
and writes it to a file. The second map reduce job reads the carrier code from this file and computes the median price 
per week for this carrier code. The output of this job is then used by a second R script which generates a 
report containing the plots for Median Price vs Year. There are two plots in the report, one for N=1 , and one for N=200.


1. LIST OF FILES and FOLDERS PROVIDED:
	
	1. An A4_PseudoMR.tar.gz folder. Unzip this folder. The folder unpacks it to a A4_PseudoMR folder. This folder 
	   contains the following files
		1.1  README.md (Description of Submission in Markdown Format)
		1.2  Makefile (Makefile for the project)
		1.3  Assignment4_Report.pdf (Report for the project in PDF)
		1.4  Assignment4_Report.Rmd (Rmd file for the Report)
		1.5  CarrierCount.java (Java file for the First Map Reduce job)
		1.6  PrintPrice.java (Java File for the 2nd Map Reduce job)
		1.7  linearReg.R (An R script for calculating the cheapest airline)
		1.8  doc - Java doc created, open index.html in a browser

	2. An A4_Cloud.tar.gz folder. Unzip this folder. The folder unpacks it to a A4_Cloud folder. This folder 
	   contains the following files
		1.1  README.md (Description of Submission in Markdown Format)
		1.2  Makefile (Makefile for the project)
		1.3  Assignment4_Report.pdf (Report for the project in PDF)
		1.4  Assignment4_Report.Rmd (Rmd file for the Report)
		1.5  CarrierCount.java (Java file for the First Map Reduce job)
		1.6  PrintPrice.java (Java File for the 2nd Map Reduce job)
		1.7  linearReg.R (An R script for calculating the cheapest airline)
		1.8  clusterCompleteCheck.sh (Shell script for checking whether job is complete in cluster)
		1.9  doc - Java doc created, open index.html in a browser


2. SYSTEM REQUIREMENTS:
	2.1. Java 1.7.0_79
	2.2. Ubuntu 14.04 64-bit/ Mac 10.11.2 64-bit
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
	
	For Running in Pseudo MR:
		3.1 Navigate to the A4_PseudoMR folder.
		3.2 Make necessary changes in the configuration parameters of the MakeFile.
		3.3 Type 'make pseudo' in the terminal

	For Running in AWS Cloud:
		3.1 Navigate to the A4_Cloud folder.
		3.2 Make necessary changes in the configuration parameters of the MakeFile.
		3.3 Type 'make cloud' in the terminal
		

4. LIST OF Files and Folders(Generated only after run of repective programs):
	
	4.1 For Pseudo MR:
		4.1.1 output/mr1_output - For the first Map Reduce Job
		4.1.2 output/mr2_output - For the second Map Reduce Job
		4.1.3 best_airline.txt - contains the carrier code for the cheapest airline
		4.1.4 Assignment4_Report.Rmd.pdf - The pdf report generated for the program

	4.2 For Cloud:
		4.2.1 output/mr1_output - For the first Map Reduce Job
		4.2.2 output/mr2_output - For the second Map Reduce Job
		4.2.3 Assignment4_Report.Rmd.pdf - The pdf report generated for the program
		4.2.4 clusterId - containing the id of the cluster created for the first MR job
		4.2.5 clusterId2 - containing the id of the cluster created for the second MR job
		4.2.6 logs (Folder for files containing time elapsed for AWS EMR jobs