import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FlightMapper extends Mapper<Object, Text, Text, Text>{
		/**
		 * Map to have a list of average price, and scheduled flight time and set 
		 * it with the corresponding key(carrier code and year)
		 */
		Configuration conf = context.getConfiguration();
        String type = conf.get("pipelineType");
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			// This variable stores the data of each record in the form of a String array
			CSVParser csvParser = new CSVParser(',','"');
			String[] flightRecordList = csvParser.parseLine(value.toString());
			boolean initialCheck = (flightRecordList.length == 110);	
			if (type.equals("test")){
				flightRecordList = Arrays.copyOfRange(flightRecordList, 1, flightRecordList.length);
				initialCheck = (flightRecordList.length == 111);	
			}
			if (initialCheck){
				// Check whether the flight is sane			
				Integer code = isSane(flightRecordList);
				// Code 0 is returned when the flight is sane
				if (code.equals(0)){
					try{
						// Setting the unique carrier code, year as key
						String mapKey = flightRecordList[6];
						String[] popularAirports = {"ATL", "ORD", "DFW", "LAX", "DEN", "IAH", "PHX", 
													"SFO", "CLT", "DTW", "MSP", "LAS", "MCO", "EWR", 
													"JFK", "LGA", "BOS", "SLC", "SEA", "BWI", "MIA", 
													"MDW", "PHL", "SAN", "FLL", "TPA", "DCA", "IAD", 
													"HOU"};
						String features = 	flightRecordList[10] + "_" + flightRecordList[5] + "_" + flightRecordList[29] + "\t" +
											flightRecordList[0] + "\t" + flightRecordList[1] + "\t" + 
											flightRecordList[2] + "\t" +
											binDayToWeek(flightRecordList[3]) + "\t" + 
											flightRecordList[4] + "\t" + 
											isPopularAirport(flightRecordList[14], popularAirports) + "\t" + 
											isPopularAirport(flightRecordList[23], popularAirports)  + "\t" + 
											binScheduledTime(flightRecordList[29]) + "\t" + 
											binScheduledTime(flightRecordList[40])  + "\t" + 
											flightRecordList[50] + "\t" + flightRecordList[55];
						if (type.equals("test")){
							context.write(new Text(mapKey), new Text(features));
						}
						else{
							String label = isDelay(flightRecordList[42]);
							if (!label.equals("NA")){
								context.write(new Text(mapKey), new Text(features + "\t" + label));
							}
						}
					}
					// Debugger to check errors
					catch(ArrayIndexOutOfBoundsException e){
						System.err.println("Error.." + e.getMessage());
					}
				}
			}
		}
		
		public static String isPopularAirport(String airport, String[] airportArray){
			for(String s: airportArray){
				if(s.equals(airport))
					return "1";
			}
			return "0";
		}

		public static String binDayToWeek(String dayOfMonth){
			Integer week = Integer.parseInt(dayOfMonth)/7;
			return week.toString();
		}

		public static String binScheduledTime(String scheduledTime){
			Integer time = Integer.parseInt(scheduledTime)/600;
			return time.toString();
		}

		public static String isDelay(String arrDelay){
			if (arrDelay.length() != 0 && arrDelay != null){
				int retval = Double.compare(Double.parseDouble(arrDelay), 0.0);
				if (retval > 0){
					return "1";
				}
				else{
					return "0";
				}
			}
			return "NA";
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
					return 1;
				}
			}else {
				// for debugging purposes
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