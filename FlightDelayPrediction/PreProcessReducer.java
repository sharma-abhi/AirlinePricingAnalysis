import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * <h1>Hadoop Reducer</h1>
 * The PreProcessReducer is responsible for simply writing the output to text files. 
 * @author: Sharma, Abhijeet and Khan, Afan Ahmad
 * @version: 4.0
 * @since: January 28, 2016
 */
public class PreProcessReducer extends Reducer<Text, Text, Text, Text>{
		/**
		 * Reduce method  -  This method writes the key and the values to the output
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

