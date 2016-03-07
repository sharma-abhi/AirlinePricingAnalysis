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
/*
 * <h1>Hadoop Driver</h1>
 * The PreProcessFlight Program is responsible for sanitizing the Input Files and 
 * Output the required columns in a text file.
 * 
 * @author: Sharma, Abhijeet and Khan, Afan Ahmad
 * @version: 4.0
 * @since: January 28, 2016
 */
public class PreProcessFlight{	
	/**
	 * Main method of the MR Job.
	 * Sets up the configuration and job objects for the MR Job.
	 * @param args InputFilePath, OutputFilePath, "train"/"test" job type.
	 */
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("pipelineType", args[2]);	//sets up a config variable as "train"/"test"
		Job job = Job.getInstance(conf, "Flight Sanity Check");
		job.setJarByClass(PreProcessFlight.class);		
		job.setMapperClass(PreProcessMapper.class);
		job.setReducerClass(PreProcessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}