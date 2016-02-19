/**
 * @author: Abhijeet Sharma, Afan Ahmad Khan
 * @version: 3.0
 * @released: February 13, 2016
 * This class represents the first MR job of the two MR jobs used in the program.
 * This class is responsible for sending to the R script the average price and the 
 * scheduled flight time for each carrier code in a particular year
 * The program takes a hadoop directory path as input containing multiple(337)
 * gzipped csv files
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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

		// Initialize the variable, for holding the carrier code and year
		private String mapKeyOrigin = "";
		private String mapKeyDestn = "";

		/**
		 * Map to have a list of average price, and scheduled flight time and set 
		 * it with the corresponding key(carrier code and year)
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// This variable stores the data of each record in the form of an array
			String[] itr = null;
			// Initializing the Value variable for storing the average price and scheduled flight time
			//PriceTime pt = new PriceTime();
			// To clean the values and splitting them with ','
			itr = value.toString().replaceAll("\"","").split(",");

			// Check whether the flight is sane
			Integer code = isSane(itr);
			// Code 0 is returned when the flight is sane
			if (code.equals(0)){
				try{
					// Logic to check whether Average Price is present for the particular flight or not
					if (itr.length == 112 && itr[111] != ""){
						// Code block to debug errors
						if (itr[0] == ""){
							//System.err.println("Error in the input string");;
						}
						else{
							// Text which contains a concatenated value of Average Price and scheduled flight time
//							
							// dep time - 32
							// arrival time - 43
							// crs dep time - 31
							// crs arr time - 42
							// cancelled - 49
							
							// Setting the carrier code and flight year as the key
							mapKeyOrigin = itr[8] + "," + itr[0] + "\t" + itr[14]   ;
							mapKeyDestn = itr[8] + "," + itr[0] + "\t" + itr[24]  ;
							// Mapper output -> carrier code and flight year as the key, average price and flight time as the value
							context.write(new Text(mapKeyOrigin), new Text("origin" + "\t" + itr[32] + "\t" + itr[43] 
									+ "\t" + itr[31] + "\t" + itr[42] + "\t" + itr[49]));
							context.write(new Text(mapKeyDestn), new Text("destn" + "\t" + itr[32] + "\t" + itr[43] 
									+ "\t" + itr[31] + "\t" + itr[42] + "\t" + itr[49]));
						}
					}
				}
				// Debugger to check errors
				catch(ArrayIndexOutOfBoundsException e){
					//System.err.println("Error.." + e.getMessage());
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
	public static class FlightReducer extends Reducer<Text, Text, Text, Text> {

		/**
		 * Reduce method  -  This method writes the key(flight code and year) and the values
		 * (average price and scheduled flight time) to the output
		 * 
		 * 
		 */
		
//		HashMap<String,ArrayList<String>> mapValuesOrigin = new HashMap<String,ArrayList<String>>();
//		HashMap<String,ArrayList<String>> mapValuesDestn = new HashMap<String,ArrayList<String>>();
		
		ArrayList<Flight> originList = new ArrayList<Flight>();
		ArrayList<Flight> destnList = new ArrayList<Flight>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Flight fl = new Flight();
			
			// Iterate through the set of values for the current carrier code and year
			for (Text val: values){
				String[] arr = val.toString().split("\t");
				if (arr[1] == "" || arr[2] == "" || arr[3] == "" || arr[4] == ""){
					System.out.println("EROOOOOORRRRRRRRRRRR");
				}
					fl.setLocation(arr[0]);
					fl.setDepTime(arr[1]);
					fl.setArrTime(arr[2]);
					fl.setCrsDepTime(arr[3]);
					fl.setCrsArrTime(arr[4]);
					fl.setCancelled(arr[5]);
					if (fl.getLocation().equalsIgnoreCase("origin")){
						originList.add(fl);
					}else {
						destnList.add(fl);
					}
				}
			
			System.out.println("Finished with Iteration ******************************************");
			int missed = 0, connection = 0, noconnection = 0;

			for (Flight fd : destnList) {
				for (Flight fo : originList){
					int crsTimeDiff = calcTimeDiffMins(fo.getCrsDepTime(),fd.getCrsArrTime());
					int actTimeDiff = calcTimeDiffMins(fo.getDepTime(), fd.getArrTime());
					if (actTimeDiff < 30){
						missed +=1;
					}else if (crsTimeDiff <= 360 && crsTimeDiff >= 30) {
						if (fd.getCancelled().equals(1)){
							missed += 1;
						}else {
							connection += 1;
						}
					}else {
						noconnection += 1;
					}
				}
//				System.out.println("ORIGIN LIST VALUES  *********");
//				System.out.println(originList);
				//addAllConnections(location,depTime,arrTime,crsDepTime,crsArrTime,cancelled);
			}
//			HashMap<String, String> hm = new HashMap<String, String>();
//			String[] keyArr = key.toString().split("\t");
//			hm.put(keyArr[0], value);
			System.out.println("FINISHEDDDDDDDDDDDD");
			context.write(key, new Text(String.valueOf(connection)));
		}
		
		private int calcTimeDiffMins(String depTime, String arrTime) {
			if (depTime != "" && arrTime != "" && depTime.length() != 0 && arrTime.length() != 0 && depTime != null  && arrTime != null ){
				Integer dTime = Integer.parseInt(depTime);
				Integer aTime = Integer.parseInt(arrTime);
				// Logic to split the Arrival and Departure hours from CRS time
				Integer crsArrTimeHour = aTime / 100;
				Integer crsDepTimeHour = dTime / 100; 
				// Logic to split the Arrival and Departure minutes from CRS time
				Integer crsArrTimeMin = aTime % 100;
				Integer crsDepTimeMin = dTime % 100;
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
				return ((crsHourDiff * 60) + (crsArrTimeMin - crsDepTimeMin));
			}else {
				return 0;
//				System.out.println("I AM HERE - THE BUG");
//				return 0;
			}
			
		}

//		private void addAllConnections(String location, String depTime, String arrTime, String crsDepTime,
//				String crsArrTime, String cancelled) {
//			if (location.equalsIgnoreCase("origin")){
//				addToOriginMap(location,depTime, arrTime, crsDepTime, crsArrTime, cancelled);
//			}else {
//				addToDestnMap(location,depTime, arrTime, crsDepTime, crsArrTime, cancelled);
//			}
//			
//		}
//		
//
//		private void addToOriginMap(String location, String depTime, String arrTime, String crsDepTime,
//				String crsArrTime, String cancelled) {
//			if (mapValuesOrigin.containsKey(location)){
//				mapValuesOrigin.get(location).add(depTime + "\t" + arrTime + "\t" + crsDepTime + "\t" + crsArrTime + "\t" + cancelled);
//			}else {
//				ArrayList<String> l = new ArrayList<String>();
//				l.add(depTime + "\t" + arrTime + "\t" + crsDepTime + "\t" + crsArrTime + "\t" + cancelled);
//				mapValuesOrigin.put(location,l);
//			}
//		}
//
//		private void addToDestnMap(String location, String depTime, String arrTime, String crsDepTime,
//				String crsArrTime, String cancelled) {
//			if (mapValuesDestn.containsKey(location)){
//				mapValuesDestn.get(location).add(depTime + "\t" + arrTime + "\t" + crsDepTime + "\t" + crsArrTime + "\t" + cancelled);
//			}else {
//				ArrayList<String> l = new ArrayList<String>();
//				l.add(depTime + "\t" + arrTime + "\t" + crsDepTime + "\t" + crsArrTime + "\t" + cancelled);
//				mapValuesDestn.put(location,l);
//			}
//		}

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

	/**
	 * Main code block to check whether the flight is sane
	 * Status Code 0 indicates Sane Flights
	 * Status Code 1 indicates Insane Flights
	 * Status Code 2 indicates Wrong Format of the fields(Exceptions)
	 * @param record
	 * @return
	 */
	public static Integer isSane(String[] record){
		try{
			Integer crsArrTime = Integer.parseInt(record[42]);
			Integer crsDepTime = Integer.parseInt(record[31]);
			Integer crsElapsedTime = Integer.parseInt(record[52]);
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
			Integer originStateFips = Integer.parseInt(record[18]);
			Integer originWAC = Integer.parseInt(record[20]);
			Integer destAirportID = Integer.parseInt(record[21]);
			Integer destAirportSeqID = Integer.parseInt(record[22]);
			Integer destCityMarketID = Integer.parseInt(record[23]);
			Integer destStateFips = Integer.parseInt(record[28]);
			Integer destWAC = Integer.parseInt(record[30]);
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
		String originStateABR = record[17];
		String originStateNM = record[19];
		String dest = record[24];
		String destCityName = record[25];
		String destStateABR = record[27];
		String destStateNM = record[29];
		// Origin, Destination,  CityName, State, StateName 
		// should not be empty
		if (!origin.equals("") && !originCityName.equals("") && 
				!originStateABR.equals("") && !originStateNM.equals("") && 
				!dest.equals("") && !destCityName.equals("") && 
				!destStateABR.equals("") && !destStateNM.equals("")){
			Integer cancelled = Integer.parseInt(record[49]);
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
		Integer arrTime = Integer.parseInt(record[43]);
		Integer depTime = Integer.parseInt(record[32]);
		Integer actualElapsedTime = Integer.parseInt(record[53]);
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
		Integer arrDelay = (int)Double.parseDouble(record[44]);
		Integer arrDelayMinutes = (int)Double.parseDouble(record[45]);
		Integer arrDel15 = (int)Double.parseDouble(record[46]);
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
	private static Integer findActualTimeZone(Integer arrTime, Integer depTime, Integer actualElapsedTime) {
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