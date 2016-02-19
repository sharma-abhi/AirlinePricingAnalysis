/**
 * @author: Abhijeet Sharma, Afan Ahmad Khan
 * @version: 2.0
 * @released: February 6, 2016
 * The program takes a hadoop directory path as input containing multiple
 * gzipped csv files and creates a plot displaying the average ticket prices
 * of all airlines per month(restricted for airlines active in 2015).
 */
import java.io.IOException;
import java.io.*;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CarrierCount{	

	// Mapper class in the Map-Reduce model
	public static class FlightMapper extends Mapper<Object, Text, Text, Text>{

		// Initialize the text variable, for holding the carrier codes
		private Text carrierCode = new Text();

		//Map to have a list of average price, month and year and set it with the corresponding key(carrier code)
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// This variable stores the data of each record in the form of an array
			String[] itr = null;
			// To clean the values and splitting them with ','
			itr = value.toString().replaceAll("\"","").split(",");
			// Initializing the text variable
			Text priceMonthYear = null;
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
							// Text which contains a concatenated value of Average Price, Month and Year
							priceMonthYear = new Text(itr[111] + ";" + itr[2] + ";" + itr[0]);
							// Setting the carrier code as the key
							carrierCode.set(itr[8]);
							// Mapper output -> carrier code as the key, price, month and year for that carrier as the value
							context.write(carrierCode, priceMonthYear);
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

	// Reduce class of Map-Reduce model - This reducer class computes the mean price for the flights
	public static class MeanReducer extends Reducer<Text, Text, Text, Text> {

		// Reduce method to merge the values of a particular carrier
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			// Boolean value to check whether the flight is active in 2015 or not.
			Boolean isActive = false;
			// Map to put the month and the sum of average prices
			HashMap<Integer, ArrayList<Double>> monthPriceMap= new HashMap<Integer, ArrayList<Double>>();
			// Iterate through the set of values for the current carrier code
			for (Text val:values){
				// Get the array of year, month and ticket prices for the particular carrier
				String[] arr = val.toString().split(";");
				Integer year = 0;
				Integer month = 0;
				Double ticketP = 0.0;
				if (arr.length == 3){
					year = Integer.parseInt(arr[2]);
					month = Integer.parseInt(arr[1]);
					ticketP = Double.parseDouble(arr[0]);
				}
				else{
					System.err.println("The values are not proper for this " + key + "carrier");
					System.err.println(val.toString());
				}
				// Set the isActive boolean if the year is 2015
				if (year.equals(2015)){
					isActive = true;
				}
				// To update the average price value for a particular month
				ArrayList<Double> priceList = new ArrayList<Double>();
				if (monthPriceMap.containsKey(month)){
					priceList = monthPriceMap.get(month);
				}
				// Aggregate all the price values into one list for the month
				priceList.add(ticketP);
				// Put the list back to the map
				monthPriceMap.put(month, priceList);
			}
			if (isActive){
				// Code to calculate the sum of all average price for a particular carrier for a particular month
				for (Integer mapKey: monthPriceMap.keySet()){
					ArrayList<Double> priceList = monthPriceMap.get(mapKey);
					// Initializing the sum and the count variables for computing mean
					Double sum = 0.0;
					Integer count = priceList.size();
					// Summing up all the ticket prices in the list
					for (Double ticketPrice: priceList){
						sum += ticketPrice;
					}
					// Calculating the mean
					Double meanPrice = sum/count;
					// Writing the result to the output - carrier code, month and mean price
					context.write(key, new Text(mapKey.toString() + "\t" + meanPrice.toString()));
				}
			}
		}
	}

	// Reduce class of Map-Reduce model - This reducer class computes the median price for the flights
	public static class MedianReducer extends Reducer<Text, Text, Text, Text> {

		// Reduce method to merge the values of a particular carrier
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			// Boolean value to check whether the flight is active in 2015 or not.
			Boolean isActive = false;
			// Map to put the month and the sum of average prices
			HashMap<Integer, ArrayList<Double>> monthPriceMap= new HashMap<Integer, ArrayList<Double>>();
			// Iterate through the set of values for the current carrier code
			for (Text val:values){
				// Get the array of year, month and ticket prices for a particular carrier
				String[] arr = val.toString().split(";");
				Integer year = 0;
				Integer month = 0;
				Double ticketP = 0.0;
				if (arr.length == 3){
					year = Integer.parseInt(arr[2]);
					month = Integer.parseInt(arr[1]);
					ticketP = Double.parseDouble(arr[0]);
				}
				else{
					System.err.println("The values are not proper for this " + key + "carrier");
					System.err.println(val.toString());
				}
				// Set the isActive boolean if the year is 2015
				if (year.equals(2015)){
					isActive = true;
				}
				// To update the average price value for a particular month
				ArrayList<Double> priceList = new ArrayList<Double>();
				if (monthPriceMap.containsKey(month)){
					priceList = monthPriceMap.get(month);
				}
				// Aggregate all the price values into one list for the month
				priceList.add(ticketP);
				// Put the list back to the map
				monthPriceMap.put(month, priceList);
			}
			if (isActive){
				// Code to calculate the sum of all average price for a particular carrier for a particular month
				for (Integer mapKey: monthPriceMap.keySet()){
					ArrayList<Double> priceList = monthPriceMap.get(mapKey);
					// Calculating the median
					Double median = findMedian(priceList);
					// Writing the result to the output - carrier code, month and median price
					context.write(key, new Text(mapKey.toString() + "\t" + median.toString()));
				}
			}
		}

		// Code to find median of the ticket price for each carrier for a particular month
		private Double findMedian(ArrayList<Double> pricelist) {
			try {
				// Sort using Java Collections.sort method
				Collections.sort(pricelist);
				// Find whether the length list of ticket prizes is even or odd
				if (pricelist.size() % 2 == 0){
					double low = pricelist.get(pricelist.size()/2  - 1);
					double high = pricelist.get(pricelist.size()/2 + 1);
					// Return the mean of the middle two values
					return (low+high)/2.0;
				}else {
					// Return the middle value
					return pricelist.get((pricelist.size() + 1)/2 -1);
				}
			}catch (Exception e){
				System.err.println("Median value is not proper " +e.getMessage());
				return 0.0;
			}
		}
	}

	// Reduce class of Map-Reduce model - This reducer class computes the fast median price for the flights
	public static class FastMedianReducer extends Reducer<Text, Text, Text, Text> {

		// Reduce method to merge the values of a particular carrier
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			// Boolean value to check whether the flight is active in 2015 or not.
			Boolean isActive = false;
			// Map to put the month and the sum of average prices
			HashMap<Integer, ArrayList<Double>> monthPriceMap= new HashMap<Integer, ArrayList<Double>>();
			// Iterate through the set of values for the current carrier code
			for (Text val:values){
				// get the array of year, month abd ticket prices for a particular carrier
				String[] arr = val.toString().split(";");
				Integer year = 0;
				Integer month = 0;
				Double ticketP = 0.0;
				if (arr.length == 3){
					year = Integer.parseInt(arr[2]);
					month = Integer.parseInt(arr[1]);
					ticketP = Double.parseDouble(arr[0]);
				}
				else{
					System.err.println("The values are not proper for this " + key + "carrier");
					System.err.println(val.toString());
				}
				// Set the isActive boolean if the year is 2015
				if (year.equals(2015)){
					isActive = true;
				}
				// To update the average price value for a particular month
				ArrayList<Double> priceList = new ArrayList<Double>();
				if (monthPriceMap.containsKey(month)){
					priceList = monthPriceMap.get(month);
				}
				// Aggregate all the price values into one list for the month
				priceList.add(ticketP);
				// Put the list back to the map
				monthPriceMap.put(month, priceList);
			}
			if (isActive){
				// Code to calculate the sum of all average price for a particular carrier for a particular month
				for (Integer mapKey: monthPriceMap.keySet()){
					ArrayList<Double> priceList = monthPriceMap.get(mapKey);
					// Initializing a variable k, to find the kth lowest value in the list
					int k = 0;
					// To check whether the list size is even or odd
					if (priceList.size()%2 == 0){
						k = priceList.size()/2;
					}else {
						k = priceList.size()/2 + 1;
					}
					// Initializing an array to store the list values
					double[] priceArr = new double[priceList.size()];
					for (int i = 0; i < priceList.size(); i++){
						priceArr[i] = priceList.get(i);
					}
					// Calculating the fast median value
					Double fastMedian = findFastMedian(priceArr,k);
					// Writing the result to the output - carrier code, month and the fast median price
					context.write(key, new Text(mapKey.toString() + "\t" + fastMedian.toString()));
				}
			}
		}

		// Code to find fast median for an array of values using QuickSelect Algorithm. This algorithm is referred from Adam Horvath's blog.
		// QuickSelect Algorithm computes the median value very quickly since this method doesn't required the entire list to be sorted.
		// It find's the median value by following an approach similar to Binary Search.
		public static double findFastMedian(double[] priceArray, int medianIndex) {

			// Initializing the start index and the end index of the array
			int startIndex = 0;
			int endIndex = priceArray.length - 1;
			// Loop to check whether startIndex is lesser than endIndex
			while (startIndex < endIndex) {
				// Temp variables to store the index positions
				int lower = startIndex;
				int higher = endIndex;
				double middleValue = priceArray[(lower + higher) / 2];
				// Loop to check whether lower is lesser than higher
				while (lower < higher) {
					// Check to see if the lower index value is greater than the middle value
					if (priceArray[lower] >= middleValue) {
						// Swap the values at the lower and the higher indexes
						double tempValue = priceArray[higher];
						priceArray[higher] = priceArray[lower];
						priceArray[lower] = tempValue;
						higher--;
					} else {
						lower++;
					}
				}
				// Resetting the lower index value
				if (priceArray[lower] > middleValue)
					lower--;

				// Resetting the lower index 
				if (medianIndex <= lower) {
					endIndex = lower;
				} else {
					startIndex = lower + 1;
				}
			}
			return priceArray[medianIndex];
		}
	}


	// Main method to start Mapper and Reducer Model
	public static void main(String[] args)	 throws Exception{
		// Initializing configuration
		Configuration conf = new Configuration();
		// Initializing the job
		Job job = Job.getInstance(conf, "carrier count");
		// Setting the jar for the job
		job.setJarByClass(CarrierCount.class);
		// Setting the mapper class
		job.setMapperClass(FlightMapper.class);
		// Code to set the reducer according to the parameters - mean, median or fast
		if (args[2].equals("mean")){
			job.setReducerClass(MeanReducer.class);
		}else if (args[2].equals("median")){
			job.setReducerClass(MedianReducer.class);
		}else if(args[2].equals("fast")){
			job.setReducerClass(FastMedianReducer.class);
		}
		// For invalid input arguments
		else{
			System.out.println("Error! Invalid Arguments");
			System.exit(0);
		}
		// Setting the output key class
		job.setOutputKeyClass(Text.class);
		// Setting the output value class
		job.setOutputValueClass(Text.class);
		// 1st argument  - gives the path to the input directory
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 2nd argument  - gives the path to the output directory
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// Status code for the system to exit once the job completes
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}

	// Main code block to check whether the flight is sane
	// Status Code 0 indicates Sane Flights
	// Status Code 1 indicates Insane Flights
	// Status Code 2 indicates Wrong Format of the fields(Exceptions)
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

	// Function to check whether the ID fields exist or not
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

	// Function to check whether Origin and Destination fields exist
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

	// Function to check for time zone difference
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

	// Function to check for ArrDelay Fields
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

	// Helper method to calculate the CRS time zone
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

	// Helper method to calculate the Actual time zone
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