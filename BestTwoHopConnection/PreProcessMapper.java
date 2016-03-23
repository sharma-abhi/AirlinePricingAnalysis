import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * <h1>Hadoop Mapper</h1>
 * The PreProcessMapper is responsible for sanitizing the Input Files and 
 * Output the required columns in a text file.
 * 
 * @author: Abhijeet, Afan, Deepen, Akshay
 * @version: 5.0
 * @since: January 28, 2016
 */

public class PreProcessMapper extends Mapper<Object, Text, Text, Text>{
	/**
	 * `FlightConnectionMapper` class will output the two intermediate set of key-value pairs where,
	 * key -> Combination of Carrier-Year-Origin and Carrier-Year-Destination.
	 * value -> Tab separated values containing arrival times, departure times and flag indicating whether flight was missed.
	 */

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		/**
		 * Map method which checks for sanity and sends required columns(features) to reducer.
		 * @param   key object
		 * @param   value A record of Input data as Text
		 * @param   context map context sent by Job
		 */

		boolean isTest = context.getConfiguration().get("mode").equals("test");        
		CSVParser csvParser = new CSVParser(',','"');
		String[] parsedString = csvParser.parseLine(value.toString());
		Flight flight = FlightManager.getFlightDetails(parsedString);

		// Process only sane flights
		if (FlightManager.isSane(flight, isTest)) {
			try {
				Text keyArriveFlight =  new Text (flight.getUniqueCarrier().trim() + "\t" + flight.getDest());          // arriving flight      
				Text keyDepartFlight =  new Text (flight.getUniqueCarrier().trim() + "\t" + flight.getOrigin());      // departing flight
				String[] features = getFeatures(flight, isTest).split("_");                                
				context.write(keyDepartFlight, new Text(features[0]));    // Write Depart key-value pair to context
				context.write(keyArriveFlight, new Text(features[1]));    // Write Arrive key-value pair to context
			}            
			catch (Exception e) {
				//Bad Data Format
				// System.err.println("Bad Data Format in Sane Records. " + e.getMessage());
			}
		}
	}

	/**
	 * Fetches the required columns from Flight object as features.
	 * @param fl Flight data object
	 * @param isTest boolean value whether the Job is for Test or Train.
	 * @return result Feature columns for prediction model
	 */
	public String getFeatures(Flight fl,boolean isTest) {

		long crsDepartTimeOfFlight = convertToLong(fl.getCrsDepTime(),fl.getFlightDate()); // Scheduled Depart Time for departing Flight(long mins format)
		long crsArrivalTimeOfFlight = convertToLong(fl.getCrsArrTime(),fl.getCrsDepTime(),fl.getFlightDate()); // Scheduled Arrival Time for arriving Flight(long mins format)
		String commonFeatures = fl.getDayOfWeek()+ 
				"\t"+fl.getMonth()+
				"\t"+fl.getDistanceGroup()+
				"\t"+fl.getDayOfMonth();
		String departFlightFeatures = commonFeatures;
		String arriveFlightFeatures = commonFeatures;        
		departFlightFeatures +=  
				"\t"+fl.getCrsDepTime() +                                                // Scheduled Depart Time for departing Flight(String format)
				"\t"+crsDepartTimeOfFlight +   
				"\t"+fl.getDest() +                                                      // destination airport of departing flight
				"\t"+"d";                                                              // "d" for departing flight
		arriveFlightFeatures += 
				"\t"+fl.getCrsArrTime() +                                                                     // Scheduled Arrival Time for arriving Flight(String format)
				"\t"+crsArrivalTimeOfFlight +     
				"\t"+fl.getOrigin() +                                                                         // origin airport of arriving flight
				"\t"+"a";                                                                                     // "a" for arriving flight
		if (!isTest){
			departFlightFeatures += "\t"+convertToLong(fl.getDepTime(),fl.getFlightDate()).toString();                     // Actual Depart Time for departing Flight
			arriveFlightFeatures  +=  "\t"+convertToLong(fl.getArrTime(), fl.getDepTime(), fl.getFlightDate()).toString(); // Actual Arrival Time for arriving Flight
		} else {
			departFlightFeatures += 
					"\t"+crsArrivalTimeOfFlight+
					"\t"+fl.getYear()+
					"\t"+fl.getFlightNumber();
			arriveFlightFeatures += 
					"\t"+crsDepartTimeOfFlight+
					"\t"+fl.getYear()+
					"\t"+fl.getFlightNumber();
		}
		return departFlightFeatures + "_" + arriveFlightFeatures;
	}

	/**
	 * Converts the departure time and departure date into DateTime.
	 * @param strTime
	 * @param strDate
	 * @return
	 */
	public Long convertToLong(String strTime,String strDate) {
		Integer dTH = Integer.parseInt(strTime) / 100; // Departure Hour
		Integer dTM = Integer.parseInt(strTime) % 100; // Departure Minute
		String finalDate = strDate.trim()+" "+dTH+":"+dTM;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Date d1 = null;
		try {
			d1 = format.parse(finalDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return d1.getTime();
	}

	/**
	 * Converts the arrival time and arrival date into DateTime by adjusting based on departure time.
	 * @param strATime
	 * @param strDTime
	 * @param strDate
	 * @return
	 */
	public Long convertToLong(String strATime,String strDTime, String strDate) {
		Integer aTH = Integer.parseInt(strATime) / 100; // Arrival Hour
		Integer aTM = Integer.parseInt(strATime) % 100; // Arrival Minute
		Integer dTH = Integer.parseInt(strDTime) / 100; // Departure Hour
		String finalDate = strDate.trim()+" "+aTH+":"+aTM;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Date d1 = null;
		try {
			d1 = format.parse(finalDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		long seconds;
		// If departure time is on the next day of arrival time, add one day
		if ( aTH < dTH) {
			seconds = d1.getTime() + (1 * 24 * 60 * 60 * 1000);
		}
		else seconds = d1.getTime();
		return seconds;
	}
}