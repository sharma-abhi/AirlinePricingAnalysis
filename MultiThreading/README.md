Author: Abhijeet Sharma
Date created: 20 January, 2016
Average execution time: 37.101 seconds

DESCRIPTION
The program takes a directory as input containing gzipped csv files and 
counts number of lines which are in incorrect format or fails sanity test (K)
and number of lines which are in correct format and passes sanity test(F)
It also calculates mean and median of avg_ticket_price by carrier and displays it in
console in ascending order.

1. LIST OF FILES/FOLDERS PROVIDED:
	1.1 README.md (Description of Submission in Markdown Format)
	1.2 all/* (Multiple input files)
	1.3 src/Solution.java -> Project Java File
	1.4 src/FileThread.java
	1.5 src/FlightInfo.java
	1.6 src/TestJunit.java
	1.7 src/TestRunner.java
	1.8 lib/javacsv.jar  (External Package for CSV read)
	1.9 lib/junit-4.12.jar   (Junit File for testing)
	1.10 lib/hamcrest-core-1.3.jar (Dependent file for JUnit)
	
2. SYSTEM REQUIREMENTS:
	2.1 Java 1.8.0_66
	2.2 Ubuntu 14.04 64-bit
	2.3 Atleast 4GB RAM

3. STEPS TO RUN (Note: Make sure all files/subfolders are present in the same folder before running):
	For Compilation:
		3.1 "javac -cp "lib/*" src/*.java"
	For Single Thread Execution:
		3.2 "java -d64 -Xmx4g -cp "src/:lib/*" Solution -p -input='data'"
	For Parallel Multi-Threaded Execution (Ubuntu):
		3.3 "java -d64 -Xmx4g -cp "src/:lib/*" Solution -p -input='data'"
	For Parallel Multi-Threaded Execution (If running on a Windows machine):
    	3.4 "java -d64 -Xmx4g -cp "src/;lib/*" Solution -p -input='data'" 
    For Running unit tests.
    	3.5 "java -d64 -Xmx4g -cp "src/:lib/*" TestRunner"