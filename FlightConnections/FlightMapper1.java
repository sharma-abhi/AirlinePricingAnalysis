

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * `FlightMapper` class will output the intermediate set of key-value pairs where,  
 * key -> Custom combination of Month and CarrierCode separated by tab.  
 * Example, `01\tAA` where `01` is January and `AA` is the carrier code.
 * value -> Ticket price in cents.
 * @author Deepen, Akshay
 */
public class FlightMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		CSVParser csvParser = new CSVParser(',','"');
		String[] parsedString=csvParser.parseLine(value.toString());
		Flight flight = FlightManager.getFlightDetails(parsedString);
		if (FlightManager.isSane(flight)) {
			
			Text carrier =  new Text (flight.getUniqueCarrier().trim());
			try {
				Integer scheduledMinutes = Integer.valueOf((int) Double.parseDouble(flight.getCrsElapsedTime()));
				Text outValue = new Text(flight.getYear()+"\t"+flight.getFlightDate()+"\t"+flight.getAvgTicketPrice().toString()+"\t"+scheduledMinutes.toString());
				// Write the ticket price in cents to context
				context.write(carrier, outValue);
			}
			catch (NullPointerException e) {
				//Skip Because of empty Average price
			}
		}
	}
}


