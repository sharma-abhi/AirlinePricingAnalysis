import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
/*
 * <h1>Hadoop Driver</h1>
 * The PreProcessFlight Program is responsible for sanitizing the Input Files and 
 * Output the required columns in a text file.
 * 
 * @author: Deepen, Akshay, Abhijeet, Afan
 * @version: 5.0
 * @since: January 28, 2016
 */

public class PreProcessFlight {

	/**
	 * Main method of the MR Job.
	 * Sets up the configuration and job objects for the MR Job.
	 * @param args InputFilePath, OutputFilePath, "train"/"test" job type.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mode", args[3]);
		conf.set("requestFolder", args[2]);
		Job job = Job.getInstance(conf, "Pre-process Flight Data");
		job.setJarByClass(PreProcessFlight.class);
		job.setMapperClass(PreProcessMapper.class);
		job.setReducerClass(PreProcessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		if (args[3].equals("test")){
			mergeFilesOnHadoopFileSystem(args[1], "final-clean-test-data",conf);
		}
		else{
			mergeFilesOnHadoopFileSystem(args[1], "final-clean-train-data",conf);
		}
		
	}
	
	public static void mergeFilesOnHadoopFileSystem(String outputFolder,String fileName, Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(URI.create(outputFolder), conf);
		FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(outputFolder+"//"+fileName));      
		PrintWriter writer  = new PrintWriter(fsDataOutputStream);		
		FileStatus[] fstatus = fileSystem.listStatus(new Path(outputFolder),new PathFilter() {
			@Override
			public boolean accept(Path arg0) {
				return arg0.getName().startsWith("part-r");
			}
		});
		long rows = 0l;
		for (int i=0;i<fstatus.length;i++) {
			FSDataInputStream fsDataInputStream = fileSystem.open(fstatus[i].getPath());
			BufferedReader br=new BufferedReader(new InputStreamReader(fsDataInputStream));
			String line;
			line=br.readLine();
			while (line != null){
				writer.write(line+" \n");
				line=br.readLine();
				rows++;
			}
			fsDataInputStream.close();
			br.close();
		}	
		writer.close();
		fsDataOutputStream.close();
		System.out.println("Total Number of Rows :: "+rows);
	}
}
