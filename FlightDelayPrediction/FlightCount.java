/*
 * @author: Abhijeet Sharma, Afan Ahmad Khan
 * @version: 3.0
 * @released: February 13, 2016
 * This class represents the MR Job used in the program.
 * This class is responsible for sending to the R script the number of connections 
 * and missed connections for each carrier in a particular year
 * The program takes a hadoop directory path as input containing multiple
 * gzipped csv files
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class is Main class for the first Map-Reduce job. This is responsible for sending carrier code,year,
 * average price and scheduled flight time to the R script. 
 * @author Afan, Abhijeet
 */
public class FlightCount{	
	/**
	 * Mapper class in the Map-Reduce model.
	 * This class maps the carrier code and year to the corresponding average price and scheduled flight time. 
	 * This class will output the intermediate set of key-value pairs where,
	 * key ->  custom combination of flight code and year separated by tab. 
	 * Example, `AA\2014` where `AA` is the carrier code and `2014` is the flight year
	 * value -> `292.33\120` where `292.33` is the Average Price and `120` is the CRS_ElapsedTime
	 * @author Afan, Abhijeet
	 */
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "carrier count");
		job.setJarByClass(FlightCount.class);
		conf.set("pipelineType", args[2]);
		job.setMapperClass(FlightMapper.class);
		job.setReducerClass(FlightReducerSingle.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// Defines additional single text based output 'text' for the job
 		//MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, Text.class);
		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
	
	


	

	