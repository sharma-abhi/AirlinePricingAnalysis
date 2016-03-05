import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FlightReducerSingle extends Reducer<Text, Text, Text, Text>{
		/**
		 * Reduce method  -  This method writes the key(flight code and year) and the values
		 * (average price and scheduled flight time) to the output
		 * 
		 * @param key Key sent by Mapper
		 * @param values Iterable of values sent by Mapper
		 * @param context context object sent by Job
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			// Iterate through the set of values for the current carrier code and year
			for (Text val: values){
				context.write(key, val);
			}
		}
	}

