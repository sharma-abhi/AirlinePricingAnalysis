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
	public static class FlightMapper extends Mapper<Object, Text, Text, Text>{

		/**
		 * Map to have a list of average price, and scheduled flight time and set 
		 * it with the corresponding key(carrier code and year)
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// Initialize the variable, for holding the carrier code and year
			String mapKeyOrigin = "";
			String mapKeyDestn = "";
			String val = "";

			// This variable stores the data of each record in the form of a String array
			CSVParser csvParser = new CSVParser(',','"');
			String[] flightRecordList = csvParser.parseLine(value.toString());

			// Check whether the flight is sane			
			Integer code = isSane(flightRecordList);
			// Code 0 is returned when the flight is sane
			if (code.equals(0)){
				try{
					// Logic to check whether Average Price is present for the particular flight or not
					if (flightRecordList.length == 110){
						// Check Key fields are proper
						if ((flightRecordList[0] == "") || (flightRecordList[14] == "") || (flightRecordList[23] == "")){
							System.err.println("Error in Key Fields");;
						}
						else{							
							// Setting the carrier code, year and origin/destination as key
							mapKeyOrigin = flightRecordList[8] + "\t" + flightRecordList[0] + "\t" + flightRecordList[14];
							mapKeyDestn = flightRecordList[8] + "\t" + flightRecordList[0] + "\t" + flightRecordList[23] ;
							
							String crsDepTs = "";
							String crsArrTs = "";
							try{
								crsDepTs = fetchTimestamp(flightRecordList[5], flightRecordList[29], null);
								crsArrTs = fetchTimestamp(flightRecordList[5], flightRecordList[29], flightRecordList[40]);
							}
							catch(ParseException e){
								e.printStackTrace();
							}

							String depTs = "";
							String arrTs = "";
							val =  crsDepTs + "\t" + crsArrTs + "\t" + flightRecordList[47];

							// We don't care about missed connections for flights whose deaprture has been cancelled.
							if (flightRecordList[47].equals("0")){
								try{
									depTs = fetchTimestamp(flightRecordList[5], flightRecordList[30], null);
									arrTs = fetchTimestamp(flightRecordList[5], flightRecordList[30], flightRecordList[41]);
								}
								catch(ParseException e){
									e.printStackTrace();
								}
								// Setting the CRS_DEP_TIME, CRS_ARR_TIME, CANCELLED, DEP_TIME, ARR_TIME as value
								val +=  "\t" + depTs + "\t" + arrTs;
								context.write(new Text(mapKeyOrigin), new Text("origin" + "\t" + val));
							}

							// Key: carrier code, year and origin/destination as the key and 
							// Value: "origin/dest", CRS_DEP_TIME, CRS_ARR_TIME, CANCELLED, DEP_TIME, ARR_TIME
							context.write(new Text(mapKeyDestn), new Text("dest" + "\t" + val));
						}
					}
				}
				// Debugger to check errors
				catch(ArrayIndexOutOfBoundsException e){
					System.err.println("Error.." + e.getMessage());
				}
			}
			else if(code.equals(1)){
				// for debugging purposes
				//System.err.println("Error..Flight record is not sane");
			}
			else{
				// for debugging purposes
				//System.err.println("Error..Failed sanity check");
			}
		}
	}

	/**
	 * This class iterates though the iterable array `Iterable<Text>` for all CarrierCode-Year keys. 
	 * The reduce method then writes the values (Average Price and Flight Time) to an output file.
	 * @author Afan, Abhijeet
	 */
	public static class FlightReducer extends Reducer<Text, Text, Text, Text>{

		/**
		 * Reduce method  -  This method writes the key(flight code and year) and the values
		 * (average price and scheduled flight time) to the output
		 * 
		 * @param key Key sent by Mapper
		 * @param values Iterable of values sent by Mapper
		 * @param context context object sent by Job
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			ArrayList<Flight> originList = new ArrayList<Flight>();
			ArrayList<Flight> destList = new ArrayList<Flight>();
			String[] keyArr = key.toString().split("\t");
			// Iterate through the set of values for the current carrier code and year
			for (Text val: values){
				String[] arr = val.toString().split("\t");
				Flight fl = new Flight();
				
				fl.setCrsDepTime(arr[1]);
				fl.setCrsArrTime(arr[2]);
				fl.setCancelled(arr[3]);
				if (arr.length == 5){
					fl.setDepTime(arr[4]);
					fl.setArrTime(arr[5]);	
				}

				// TODO: optimization: don't require arrival time for departing flights and vice-versa
				if (arr[0].equals("origin")){
					originList.add(fl);
				}
				else {
					destList.add(fl);
				}
			}
			
			int missed = 0, connection = 0, noconnection = 0;
			try{
				for (Flight fd : destList) {
					for (Flight fo : originList){
						System.out.println("getCancelled(): ");
						System.out.println(fd.getCrsArrTime());
						System.out.println(fo.getCrsDepTime());
						long crsTimeDiff = calcTimeDiffMins(fo.getCrsDepTime(),fd.getCrsArrTime());

						// if difference between scheduled departure and scheduled arrival time is within 30 and 360 mins (inclusive).
						if ((crsTimeDiff <= 360) && (crsTimeDiff >= 30)){
							// if arriving flight is cancelled, it's a missed connection(irrespective of whether departing flight is missed or not)
							if (fd.getCancelled().equals("1"))   
							{
								missed += 1;
							}
							else{
								if (fo.getCancelled().equals("1")){ //we don't care whether the departing flight is cancelled or not.
									noconnection += 1;
								}
								else{
									long actTimeDiff = calcTimeDiffMins(fo.getDepTime(), fd.getArrTime());
									if(actTimeDiff < 30){
										missed += 1;
									}
									else{
										connection += 1;
									}
								}
							}
						}
						else {
							noconnection += 1;
						}
					}
				}
			}
			catch(ParseException e){
				e.printStackTrace();
			}

			if ((connection != 0) && (missed != 0)){
				context.write(new Text(keyArr[0] + '\t' + keyArr[1]), new Text(String.valueOf(connection) + "\t" + String.valueOf(missed)));
			}
		
		}
	}
		
	public static long calcTimeDiffMins(String depTimestamp, String arrTimestamp) throws ParseException {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			Date depDate = sdf.parse(depTimestamp);
			Date arrDate = sdf.parse(arrTimestamp);
			return TimeUnit.MILLISECONDS.toMinutes(depDate.getTime() - arrDate.getTime());
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
		Job job = Job.getInstance(conf, "carrier count");
		// Setting the jar for the job
		job.setJarByClass(FlightCount.class);
		// Setting the mapper class
		job.setMapperClass(FlightMapper.class);
		// Code to set the reducer according to the parameters - mean, median or fast
		job.setReducerClass(FlightReducer.class);
		// Setting the output key class
		job.setOutputKeyClass(Text.class);
		// Setting the output value class
		job.setOutputValueClass(Text.class);
		// 1st argument  - gives the path to the input directory
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 2nd argument  - gives the path to the output directory
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// Status code for the system to exit once the job completes
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static String fetchTimestamp(String dateVal,String depVal, String arrVal)throws ParseException{
		//Date date = new Date();
		String dateAndTime = null;
		
		if (arrVal == null){
			dateAndTime = dateVal.concat(depVal);
		}
		else{			
			if (Integer.parseInt(arrVal) > Integer.parseInt(depVal)){
				dateAndTime = dateVal.concat(arrVal);
			}
			else{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date date = sdf.parse(dateVal);
				long milli = date.getTime() + (24 * 60 * 60 * 1000);
				date.setTime(milli);
				//sdf.parse(date)
				dateAndTime = sdf.format(date).concat(arrVal);
			}

		}
		
		return dateAndTime;
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
	 * @param arrTime
	 * @param depTime
	 * @param actualElapsedTime
	 * @return
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