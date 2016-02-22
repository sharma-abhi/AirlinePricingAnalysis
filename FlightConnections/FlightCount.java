import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This file contains the main class for the Map Reduce Job. This job computes the 
 * total number of connections, missed connections and no connections for the entire
 * flight data. This data for the corresponding career and year is then sent to a R
 * script that generated a report based on the given data.
 * @author Afan, Abhijeet
 * @version 1.0
 */
public class FlightCount{	
	/**
	 * Mapper class in the Map-Reduce model.
	 * This class writes to the context two times in order to ensure self join on origin
	 * and destination fields in the reducer field.
	 * This process makes the reducer method a lot less expensive and computation is done a 
	 * lot faster on a huge data set.
	 * This class will output the intermediate set of key-value pairs where,
	 * key  custom combination of flight code, year, origin/destination and month separated by tab. 
	 * Example, `AA\2014\JFK\11` where `AA` is the carrier code,`2014` is the flight year,
	 * 'JFK' can be the origin or destination and 11 is the month number
	 * value `arriving\20130211070200\20130211090200\0` where `arriving` is Type,`20130211070200`
	 * is the scheduled time stamp, `20130211090200` is the actual time stamp and
	 * `0` is the Cancellation status
	 */
	public static class FlightMapper extends Mapper<Object, Text, Text, Text>{	

		/**
		 * Map to have a list of average price, and scheduled flight time and set 
		 * it with the corresponding key(carrier code and year)
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			// Parsing each record and storing it in a String array
			CSVParser csvParser = new CSVParser(',','"');
			String[] flightRecordList = csvParser.parseLine(value.toString());
			// Initializing the val variable, this variable will be sent to the Reducer as text value
			String val = "";
			// Check whether the flight is sane			
			Integer code = isSane(flightRecordList);
			// Code 0 is returned when the flight is sane
			if (code.equals(0)){
				try{						
					// Setting the carrier code, year and origin/destination and flight month as key
					String mapKeyOrigin = flightRecordList[8] + "\t" + flightRecordList[0] + "\t" + flightRecordList[14] + "\t" + flightRecordList[2];
					String mapKeyDest = flightRecordList[8] + "\t" + flightRecordList[0] + "\t" + flightRecordList[23] + "\t" + flightRecordList[2];
					String cancelledStatus = flightRecordList[47];
					// Initializing the arrival time stamp value as a dummy data
					String arrTs = "DUMMY";

					// we consider "departing" flights only if flight is not cancelled 
					// we don't care about flights whose departure has been cancelled -> (no connections).
					// Calculating the scheduled time stamp
					String[] crsTs = fetchTimestamp(flightRecordList[5], flightRecordList[29], flightRecordList[40]).split("\t");

					// Sending the departure details only when the flight is not cancelled
					// since if departure flight is cancelled , there is no missed or actual connection
					if (cancelledStatus.equals("0")){
						// Calculating the actual time stamp
						String[] actualTs = fetchTimestamp(flightRecordList[5], flightRecordList[30], flightRecordList[41]).split("\t");
						// Setting the Type, CRS_DEP_TIME, DEP_TIME, CANCELLED as value string
						val =  "departing" + "\t" + crsTs[0] + "\t" + actualTs[0] + "\t" + cancelledStatus;
						context.write(new Text(mapKeyOrigin), new Text(val));
						arrTs = actualTs[1];
					}
					val =  "arriving" + "\t" + crsTs[1] + "\t" + arrTs + "\t" + cancelledStatus;
					context.write(new Text(mapKeyDest), new Text(val));
				}
				// Debugger to check errors
				catch(ParseException e){
					System.err.println("Error.." + e.getMessage());
				}
				catch(ArrayIndexOutOfBoundsException e){
					System.err.println("Error.." + e.getMessage());
				}
			}
		}
	}

	/**
	 * This class iterates though the iterable array Iterable for all CarrierCode,
	 * Year,Origin/Destination and Month keys. 
	 * The reduce method then writes the values (key, actual number of connections 
	 * and number of missed connections) to an output file.
	 * @author Afan, Abhijeet
	 */
	public static class FlightReducer extends Reducer<Text, Text, Text, Text>{

		/**
		 * Reduce method - This method writes the key(flight code, year, origin/destination
		 * and month number) and the values (actual connections and missed connections) to the output
		 * @param key Key sent by Mapper
		 * @param values Iterable of values sent by Mapper
		 * @param context Context object sent by Job
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			// Initializing the variables
			Integer missed = 0;
			Integer connection = 0;
			ArrayList<Flight> departList = new ArrayList<Flight>();
			ArrayList<Flight> arrivalList = new ArrayList<Flight>();

			// Iterating through the values
			for (Text val: values){
				String[] arr = val.toString().split("\t");
				Flight fl = new Flight();
				fl.setCrsTime(arr[1]);
				fl.setActualTime(arr[2]);
				fl.setCancelled(arr[3]);
				// Storing the data for the departing flights
				if (arr[0].equals("departing")){
					departList.add(fl);
					String[] res = compareDepartingWithAllArriving(fl, arrivalList, missed, connection).split("\t");
					missed = Integer.parseInt(res[0]);
					connection = Integer.parseInt(res[1]);
				}
				// Storing the data for the arrival flights
				else{
					arrivalList.add(fl);
					String[] res = compareArrivalWithAllDeparting(fl, departList, missed, connection).split("\t");
					missed = Integer.parseInt(res[0]);
					connection = Integer.parseInt(res[1]);
				}

			}
			// Ignoring the records for zero missed and valid connections
			if ((missed != 0) && (connection != 0)){
				context.write(key, new Text(missed + "\t" + connection));
			}
		}

		/**
		 * This method compares each arriving flight with the departing flight list and computes
		 * for the number of missed connections and actual connections
		 * @param arriveFlight  the arriving flight object
		 * @param departList  the departing flight list
		 * @param missed  the number of missed connections
		 * @param connection the number of connections
		 * @return the number of missed connections and actual connections as String
		 */
		public String compareArrivalWithAllDeparting(Flight arriveFlight, ArrayList<Flight> departList, int missed, int connection) {
			for (Flight departFlight : departList){
				long crsTimeDiff = TimeUnit.MILLISECONDS.toMinutes(Long.parseLong(departFlight.getCrsTime()) - 
						Long.parseLong(arriveFlight.getCrsTime()));
				// if difference between scheduled departure and scheduled arrival time is within 30 and 360 minutes (inclusive).
				// if difference between scheduled departure and scheduled arrival time is within 30 and 360 minutes (inclusive).
				if ((crsTimeDiff <= 360) && (crsTimeDiff >= 30)){
					// if arriving flight is cancelled, it's a missed connection(irrespective of whether departing flight is missed or not)
					if (arriveFlight.getCancelled().equals("0")){
						long actTimeDiff = 	TimeUnit.MILLISECONDS.toMinutes(Long.parseLong(departFlight.getActualTime()) - 
								Long.parseLong(arriveFlight.getActualTime()));
						System.out.println("arr cancelled: " +  arriveFlight.getCancelled());
						System.out.println("dep cancelled: " + departFlight.getCancelled());
						System.out.println("actTimeDiff: " + actTimeDiff);
						//System.out.println(actTimeDiff);
						if(actTimeDiff < 30){
							missed += 1;
						}
						else{ // actual Time Difference is more that 30 mins
							connection += 1;
						}
					}
					else{// if arrival flight is cancelled
						missed += 1;
					}
				}
				else {// if scheduled flight time difference is more than 6 hrs or less than 30 mins
					//noconnection += 1;
				}
			}	
			return missed + "\t" + connection;
		}

		/**
		 * This method compares each departing flight with the arriving flight list and computes
		 * for the number of missed connections and actual connections
		 * @param departFlight the departing flight object
		 * @param arrivalFlightList the arrival flight list
		 * @param missed  the number of missed connections
		 * @param connection the number of connections
		 * @return the number of missed connections and actual connections as String
		 */
		public String compareDepartingWithAllArriving(Flight departFlight, ArrayList<Flight> arrivalFlightList, int missed, int connection) {
			for (Flight arriveFlight : arrivalFlightList){
				long crsTimeDiff = TimeUnit.MILLISECONDS.toMinutes(Long.parseLong(departFlight.getCrsTime()) - 
						Long.parseLong(arriveFlight.getCrsTime()));
				//System.out.println("In Reducer: CRSTimeDiff");
				//System.out.println(crsTimeDiff);
				// if difference between scheduled departure and scheduled arrival time is within 30 and 360 mins (inclusive).
				// if difference between scheduled departure and scheduled arrival time is within 30 and 360 mins (inclusive).
				if ((crsTimeDiff <= 360) && (crsTimeDiff >= 30)){
					// if arriving flight is cancelled, it's a missed connection(irrespective of whether departing flight is missed or not)
					if (arriveFlight.getCancelled().equals("0")){
						long actTimeDiff = 	TimeUnit.MILLISECONDS.toMinutes(Long.parseLong(departFlight.getActualTime()) - 
								Long.parseLong(arriveFlight.getActualTime()));
						System.out.println("arr cancelled: " +  arriveFlight.getCancelled());
						System.out.println("dep cancelled: " + departFlight.getCancelled());
						System.out.println("actTimeDiff: " + actTimeDiff);
						//System.out.println(actTimeDiff);
						if(actTimeDiff < 30){
							missed += 1;
						}
						else{ // actual Time Difference is more that 30 mins
							connection += 1;
						}
					}
					else{// if arrival flight is cancelled
						missed += 1;					
					}
				}
				else {// if scheduled flight time difference is more than 6 hrs or less than 30 mins
					//noconnection += 1;
				}
			}
			return missed + "\t" + connection;
		}
	}

	/**
	 * Main method to start Mapper and Reducer Model
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args)	 throws Exception{
		// Initializing configuration
		Configuration conf = new Configuration();
		// Initializing the job
		Job job = Job.getInstance(conf, "flight count");
		// Setting the jar for the job
		job.setJarByClass(FlightCount.class);
		// Setting the mapper class
		job.setMapperClass(FlightMapper.class);
		// Code to set the reducer according to the parameters - mean, median or fast
		job.setReducerClass(FlightReducer.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// Setting the output key class
		job.setOutputKeyClass(Text.class);
		// Setting the output value class
		job.setOutputValueClass(Text.class);


		//job.setInputFormatClass(WholeFileInputFormat.class);
		//WholeFileInputFormat.addInputPath(job, new Path(args[0]));
		// 1st argument  - gives the path to the input directory
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 2nd argument  - gives the path to the output directory
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// Status code for the system to exit once the job completes
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Converting arrival and departure time to respective time stamps for the given flight date
	 * @param dateVal
	 * @param depVal
	 * @param arrVal
	 * @return
	 * @throws ParseException
	 */
	public static String fetchTimestamp(String dateVal,String depVal, String arrVal)throws ParseException{

		String depStringTs = null;
		String arrStringTs = null;
		Integer depValInt = Integer.parseInt(depVal);
		Integer depValHour = depValInt / 100;			
		Integer depValMin = depValInt % 100;
		Integer arrValInt = Integer.parseInt(arrVal);
		Integer arrValHour = arrValInt / 100;			
		Integer arrValMin = arrValInt % 100;
		// Formatting the given departure time stamp
		depStringTs = dateVal + " " + depValHour.toString() + ":" + depValMin.toString();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Date depTimestamp = sdf.parse(depStringTs);
		// If the flight arrives next day 
		if (arrValInt > depValInt){
			arrStringTs = dateVal + " " + arrValHour.toString() + ":" + arrValMin.toString();
		}
		// Formatting the given arrival time stamp
		else{
			sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = sdf.parse(dateVal);
			long milli = date.getTime() + (24 * 60 * 60 * 1000); // increment by 1 day
			date.setTime(milli);
			arrStringTs = sdf.format(date) + " " + arrValHour.toString() + ":" + arrValMin.toString();

		}
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Date arrTimestamp = sdf.parse(arrStringTs);
		// convert to milliseconds
		Long depInMS = depTimestamp.getTime();
		Long arrInMS = arrTimestamp.getTime();
		// return both the departure time and arrival time (in milliseconds)
		return depInMS.toString() + "\t" + arrInMS.toString();
	}

	/**
	 * Main code block to check whether the flight is sane
	 * Status Code 0 indicates Sane Flights
	 * Status Code 1 indicates Insane Flights
	 * Status Code 2 indicates Wrong Format of the fields(Exceptions)
	 * @param record List of Strings containing information about flight tuple
	 * @return code [0=sane, 1=insane, 2=bad format]
	 */
	public static Integer isSane(String[] record){
		try{
			Integer crsArrTime = Integer.parseInt(record[40]);
			Integer crsDepTime = Integer.parseInt(record[29]);
			Integer crsElapsedTime = Integer.parseInt(record[50]);
			// crsArrTime and CRSDepTime should not be zero
			if (crsArrTime != 0 && crsDepTime != 0){
				Integer crstimeZone = calculateCRSTimeZone(crsArrTime,crsDepTime,crsElapsedTime);
				return checkIDFieldsExist(crstimeZone,record); 
			}else {
				// for debugging purposes
				// System.err.println("Error : CRSArrTime or CRSDepTime fields are not proper");
				return 1;
			}
		}catch (Exception e){
			// for debugging purposes
			// System.err.println("Error : The record is not proper");
			return 2;
		}
	}

	/**
	 * Function to check whether the ID fields exist or not
	 * @param crstimeZone
	 * @param record
	 * @return
	 */
	private static int checkIDFieldsExist(Integer crstimeZone, String[] record) {
		// crstimeZone % 60 should be 0
		if (crstimeZone % 60 == 0)
		{
			Integer originAirportID = Integer.parseInt(record[11]);
			Integer originAirportSeqID = Integer.parseInt(record[12]);
			Integer originCityMarketID = Integer.parseInt(record[13]);
			Integer originStateFips = Integer.parseInt(record[17]);
			Integer originWAC = Integer.parseInt(record[19]);
			Integer destAirportID = Integer.parseInt(record[20]);
			Integer destAirportSeqID = Integer.parseInt(record[21]);
			Integer destCityMarketID = Integer.parseInt(record[22]);
			Integer destStateFips = Integer.parseInt(record[26]);
			Integer destWAC = Integer.parseInt(record[28]);
			// AirportID,  AirportSeqID, CityMarketID, StateFips, Wac 
			// should be larger than 0
			if ((originAirportID > 0) && (originAirportSeqID > 0) && 
					(originCityMarketID > 0) && (originStateFips > 0) && 
					(originWAC > 0) && (destAirportID > 0) && 
					(destAirportSeqID > 0) && (destCityMarketID > 0) && 
					(destStateFips > 0) && (destWAC > 0)){ 
				return checkForOriginDest(record, crstimeZone);
			}else{
				// for debugging purposes
				// System.err.println("Error : The ID fields of the record are not proper");
				return 1;
			}
		}else {
			// for debugging purposes
			// System.err.println("Error : The CRSTimeZone is not a multiple of 60");
			return 1;
		}
	}

	/**
	 * Function to check whether Origin and Destination fields exist
	 * @param record
	 * @param crstimeZone
	 * @return
	 */
	private static int checkForOriginDest(String[] record, Integer crstimeZone) {
		String origin = record[14];
		String originCityName = record[15];
		String originStateABR = record[16];
		String originStateNM = record[18];
		String dest = record[23];
		String destCityName = record[24];
		String destStateABR = record[25];
		String destStateNM = record[27];
		// Origin, Destination,  CityName, State, StateName 
		// should not be empty
		if (!origin.equals("") && !originCityName.equals("") && 
				!originStateABR.equals("") && !originStateNM.equals("") && 
				!dest.equals("") && !destCityName.equals("") && 
				!destStateABR.equals("") && !destStateNM.equals("")){
			Integer cancelled = Integer.parseInt(record[47]);
			// Check for flights that are not Cancelled(1 = yes)
			if(cancelled.equals(0)){
				return checkForArrAndDepTime(record, crstimeZone);
			}else {
				// for debugging purposes
				// System.err.println("Error : The Cancelled field is not proper");
				return 0;
			}
		}else {
			// for debugging purposes
			// System.err.println("Error : The Origin and Destination fields are not proper");
			return 1;
		}
	}

	/**
	 * Function to check for time zone difference
	 * @param record
	 * @param crstimeZone
	 * @return
	 */ 
	private static int checkForArrAndDepTime(String[] record, Integer crstimeZone) {
		Integer arrTime = Integer.parseInt(record[41]);
		Integer depTime = Integer.parseInt(record[30]);
		Integer actualElapsedTime = Integer.parseInt(record[51]);
		Integer actualTimeZone = findActualTimeZone(arrTime,depTime,actualElapsedTime);
		Integer timeZoneDiff = actualTimeZone - crstimeZone;
		//arrTime -  depTime - actualElapsedTime - timeZone should be zero
		if (timeZoneDiff == 0){
			return checkForArrDelay(record);
		}else{
			// for debugging purposes
			// System.err.println("Error : The arrTime -  depTime - actualElapsedTime - timeZone is not zero");
			return 1;
		}
	}

	/**
	 * Function to check for ArrDelay Fields
	 * @param record
	 * @return
	 */
	private static int checkForArrDelay(String[] record) {
		Integer arrDelay = (int)Double.parseDouble(record[42]);
		Integer arrDelayMinutes = (int)Double.parseDouble(record[43]);
		Integer arrDel15 = (int)Double.parseDouble(record[44]);
		int finalReturnVal = 0;
		// if ArrDelay > 0 then ArrDelay should be equal to ArrDelayMinutes
		if (arrDelay > 0){                                        
			if (arrDelay.equals(arrDelayMinutes)){
				// end of sanity check for flights that are not cancelled and
				// arrDelay > 0 and arrDelay == arrDelayMinutes .
				finalReturnVal =  0;  
			}else {
				// for debugging purposes
				// System.err.println("Error : ArrDelay is not equal to ArrDelayMinutes");
				finalReturnVal = 1;
			}
		}
		// if ArrDelay < 0 then ArrDelayMinutes should be zero
		else if(arrDelay < 0){
			if (arrDelayMinutes.equals(0)){
				// end of sanity check for flights that are not cancelled and
				// arrDelay < 0 and arrDelayMinutes equals zero.
				finalReturnVal =  0;
			}else {
				// for debugging purposes
				// System.err.println("Error : ArrayDelayMinutes is not equal to zero");
				finalReturnVal = 1;
			}
		}
		// if ArrDelayMinutes >= 15 then ArrDel15 should be 1(true)
		if (arrDelayMinutes >= 15){
			if (arrDel15.equals(1)){
				// end of sanity check for flights that are not cancelled and
				// arrDelayMinute >= 15 and arrDel15 == 1.
				finalReturnVal =  0;
			}else {
				// for debugging purposes
				// System.err.println("Error : ArrayDelay15 is not equal to 1");
				finalReturnVal = 1;
			}
		}else {
			// for debugging purposes
			// System.err.println("Error : ArrDelayMinutes is not >= 15");
			finalReturnVal =  0;
		}
		return finalReturnVal;
	}

	/**
	 * Helper method to calculate the CRS time zone
	 * @param crsArrTime
	 * @param crsDepTime
	 * @param crsElapsedTime
	 * @return
	 */
	private static Integer calculateCRSTimeZone(Integer crsArrTime, Integer crsDepTime, Integer crsElapsedTime) {
		// Logic to split the Arrival and Departure hours from CRS time
		Integer crsArrTimeHour = crsArrTime / 100;
		Integer crsDepTimeHour = crsDepTime / 100; 
		// Logic to split the Arrival and Departure minutes from CRS time
		Integer crsArrTimeMin = crsArrTime % 100;
		Integer crsDepTimeMin = crsDepTime % 100;
		// Logic to find the time difference between CRS Arrival time and Departure times
		Integer crsHourDiff;
		if (crsArrTimeHour > crsDepTimeHour){
			crsHourDiff = crsArrTimeHour - crsDepTimeHour;
		}
		else if (crsArrTimeHour == crsDepTimeHour){
			if(crsArrTimeMin > crsDepTimeMin){
				crsHourDiff = crsArrTimeHour - crsDepTimeHour;    
			}
			else{
				// Time difference when the Arrival time is next day
				crsHourDiff = (24 - crsDepTimeHour) + crsArrTimeHour;                               
			}
		}
		else{
			// Time difference when the Arrival time is next day
			crsHourDiff = (24 - crsDepTimeHour) + crsArrTimeHour;
		}                
		return ((crsHourDiff * 60) + (crsArrTimeMin - crsDepTimeMin)) - crsElapsedTime;
	}

	/**
	 * Helper method to calculate the Actual time zone
	 * @param arrTime Actual arrival time
	 * @param depTime Actual departure time
	 * @param actualElapsedTime Actual Elapsed time
	 * @return The actual time zone
	 */
	public static Integer findActualTimeZone(Integer arrTime, Integer depTime, Integer actualElapsedTime) {
		// Logic to split the Arrival and Departure hours from Actual time
		Integer arrTimeHour = arrTime / 100;
		Integer depTimeHour = depTime / 100;
		// Logic to split the Arrival and Departure hours from Actual time
		Integer arrTimeMin = arrTime % 100;
		Integer depTimeMin = depTime % 100;                                
		// Logic to find the time difference between Actual Arrival time and Departure times
		Integer hourDiff;
		if (arrTimeHour > depTimeHour){
			hourDiff = arrTimeHour - depTimeHour;
		}
		else if (arrTimeHour == depTimeHour){
			if(arrTimeMin > depTimeMin){
				hourDiff = arrTimeHour - depTimeHour;    
			}
			else{
				// Time difference when the Arrival time is next day
				hourDiff = (24 - depTimeHour) + arrTimeHour;       
			}
		}
		else{
			// Time difference when the Arrival time is next day
			hourDiff = (24 - depTimeHour) + arrTimeHour;   
		}
		return ((hourDiff * 60) + (arrTimeMin - depTimeMin)) - actualElapsedTime;
	}
}