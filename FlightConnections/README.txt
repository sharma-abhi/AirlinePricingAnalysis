Author: Abhijeet Sharma and Afan Ahmad Khan
Date created: February 20, 2016


DESCRIPTION
The program takes a hadoop directory path as input containing multiple gzipped csv files and runs a Map Reduce Job for computing the hop
paths for the flights. In this program , the concept of self join is used in the Reducer to reduce the space and time taken to compute the
number of missed connections and actual connections for the entire flight data.


1. LIST OF FILES and FOLDERS PROVIDED:
	
	1. An Sharma_Khan_A5.tar.gz folder. Unzip this folder. The folder unpacks it to a Sharma_Khan_A5 folder. This folder 
	   contains the following files
		1.1  README.txt (Description of Submission in Markdown Format)
		1.2  Makefile (Makefile for the project)
		1.3  Assignment5_Report.pdf (Report for the project in PDF)
		1.4  Assignment5_Report.Rmd (Rmd file for the Report)
		1.5  FlightCount.java (Java file for the Map Reduce Job)
		1.6  Flight.java (POJO class containing for the flight data)
		1.7  doc - Java doc created, open index.html in a browser
		1.8  commons-lang3-3.4.jar - to run the cvs parser
		1.9  CSV Parser and CSVReaderNullFieldIndicator - for parsing of the records ( this is is open source code 
		     for parsing data)
		 


2. SYSTEM REQUIREMENTS:
	2.1. Java 1.7.0_79
	2.2. Ubuntu 14.04 64-bit/ MAC 10.11 64-bit
	2.3. 8GB RAM
	2.4. Pandoc (https://github.com/jgm/pandoc/releases/tag/1.16.0.2)
	2.5. pdflatex (sudo apt-get install texlive-latex-base and sudo apt-get install texlive-fonts-recommended) (Only if running from KnitR)
	2.6. R Packages:
	2.6.1 Rcpp
	2.6.2 R.utils
	2.6.3 ggplot2
	2.6.4 rmarkdown
	2.6.5 plyr
	2.6.6 reshape2


3. STEPS TO RUN (Note: Make sure all files/subfolders are present in the same folder before running):
	NOTE: Please add the required input files in the "input" folder of hadoop(for pseudo) or "<BUCKETNAME>/input"(for cloud) before execution.
	NOTE2: Change the Configuration Parameters(found in the top) in the MakeFile before running any jobs.
	
		3.1 Navigate to the Sharma_Khan_A5 folder.
		3.2 Make necessary changes in the configuration parameters of the MakeFile.
		3.3 For Running in Pseudo MR: 
			Type 'make pseudo' in the terminal

		3.4 For Running in AWS Cloud:
			Type 'make cloud' in the terminal
		

4. LIST OF Files and Folders(Generated only after run of respective programs):
	
	4.1 For Pseudo MR:
		4.1.1 Assignment5_Report.pdf - The pdf report generated for the program

	4.2 For Cloud:
		4.2.1 output - Output Folder from the map reduce job
		4.2.2 Assignment5_Report.pdf - The pdf report generated for the program
		4.2.3 clusterId.txt - Text File containing the id of the cluster created for the MR job