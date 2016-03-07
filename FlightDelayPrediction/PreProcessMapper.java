import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * <h1>Hadoop Mapper</h1>
 * The PreProcessMapper is responsible for sanitizing the Input Files and 
 * Output the required columns in a text file.
 * 
 * @author: Sharma, Abhijeet and Khan, Afan Ahmad
 * @version: 4.0
 * @since: January 28, 2016
 */
public class PreProcessMapper extends Mapper<Object, Text, Text, Text>{
		/**
		 * Map to have a list of average price, and scheduled flight time and set 
		 * it with the corresponding key(carrier code and year)
		 */
		static String[] popularAirports = {"ATL", "ORD", "DFW", "LAX", "DEN", "IAH", "PHX", 
									"SFO", "CLT", "DTW", "MSP", "LAS", "MCO", "EWR", 
									"JFK", "LGA", "BOS", "SLC", "SEA", "BWI", "MIA", 
									"MDW", "PHL", "SAN", "FLL", "TPA", "DCA", "IAD", 
									"HOU"};
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			/**
			 * Map method which checks for sanity and sends required columns(features) to reducer.
			 * @param	key object
			 * @param	value A record of Input data as Text
			 * @param	context map context sent by Job
			 */
			Configuration conf = context.getConfiguration();
        	boolean isTestJob = conf.get("pipelineType").equals("test");		//checks "Train" or "Test" value
        	// Fetches input data into an array			
			CSVParser csvParser = new CSVParser(',','"');
			String[] flightRecordList = csvParser.parseLine(value.toString());
			boolean initialCheck = (flightRecordList.length == 110);	
			if (isTestJob){
				flightRecordList = Arrays.copyOfRange(flightRecordList, 1, flightRecordList.length);	//delete first column in test dataset (index)
				initialCheck = (flightRecordList.length == 111);	
			}
			if (initialCheck){
				// Check whether the flight is sane			
				Integer code = isSane(flightRecordList, isTestJob);
				// Code 0 is returned when the flight is sane
				if (code.equals(0)){
					try{
						// Setting the unique carrier code as key
						String mapKey = flightRecordList[6];
						String features = "";
						if (isTestJob){
							//create the test data id(for join with validation data in Spark)
							features = 	flightRecordList[10] + "_" + flightRecordList[5] + "_" + flightRecordList[29] + "\t"; 
						}
						features += flightRecordList[0] + "\t" + flightRecordList[1] + "\t" + 
									flightRecordList[2] + "\t" +
									binDayToWeek(flightRecordList[3]) + "\t" + 
									flightRecordList[4] + "\t" + 
									isPopularAirport(flightRecordList[14]) + "\t" + 
									isPopularAirport(flightRecordList[23])  + "\t" + 
									binScheduledTime(flightRecordList[29]) + "\t" + 
									binScheduledTime(flightRecordList[40])  + "\t" + 
									flightRecordList[50] + "\t" + flightRecordList[55];
						if (isTestJob){
							context.write(new Text(mapKey), new Text(features));
						}
						else{
							String label = isDelay(flightRecordList[42]);	
							if (!label.equals("NA")){				 // ignore training data when label is "NA" (empty or null)
								context.write(new Text(mapKey), new Text(features + "\t" + label));
							}
						}
					}
					catch(ArrayIndexOutOfBoundsException e){
						System.err.println("Error.." + e.getMessage());
					}
				} //close sanityCheck 
			} //close initalCheck
		} // close map
		
		public static String isPopularAirport(String airport){
			/**
			 * Checks whether given airport is a popular airport or not.
			 * @param	airport Given airport to verify.
			 * @return	"1" if given airport is a popular airport, "0" otherwise.
			 */
			for(String s: popularAirports){
				if(s.equals(airport)) return "1";
			}
			return "0";
		}

		public static String binDayToWeek(String dayOfMonth){
			/**
			 * Bins day of a month to its corresponding week number(division by 7).
			 * @param	dayOfMonth day as a String
			 * @return	Week number of the day
			 */
			Integer week = Integer.parseInt(dayOfMonth)/7;
			return week.toString();
		}

		public static String binScheduledTime(String scheduledTime){
			/**
			 * Bins time into 4 bins (division by 600).
			 * @param	scheduledTime time in hhmm format as String.
			 * @return	Bin of the given given time 
			 */
			Integer time = Integer.parseInt(scheduledTime)/600;
			return time.toString();
		}

		public static String isDelay(String arrDelay){
			/**
			 * Checks whether flight is delayed or not (arrDelay > 0)
			 * @param	arrDelay time delay as String
			 * @return	"1" if flight is delayed, "0" if not, "NA" if input is empty or null.
			 */
			if (arrDelay.length() != 0 && arrDelay != null){
				int retval = Double.compare(Double.parseDouble(arrDelay), 0.0);
				return (retval > 0) ? "1" : "0";
			}
			return "NA";
		}
		
		/**
		 * Main code block to check whether the flight is sane
		 * Status Code 0 indicates Sane Flights
		 * Status Code 1 indicates Insane Flights
		 * Status Code 2 indicates Wrong Format of the fields(Exceptions)
		 * @param record List of Strings containing information about flight tuple
		 * @return code "0" for sanity passed, "1" for sanity failed and "2" for bad format
		 */
		public static Integer isSane(String[] record, boolean isTestJob){
			try{
				Integer crsArrTime = Integer.parseInt(record[40]);
				Integer crsDepTime = Integer.parseInt(record[29]);
				Integer crsElapsedTime = Integer.parseInt(record[50]);
				// crsArrTime and CRSDepTime should not be zero
				if (crsArrTime != 0 && crsDepTime != 0){
					Integer crstimeZone = calculateCRSTimeZone(crsArrTime,crsDepTime,crsElapsedTime);
					return checkIDFieldsExist(crstimeZone,record, isTestJob); 
				}else {
					return 1;
				}
			}catch (Exception e){
				return 2;
			}
		}

		/**
		 * Function to check whether the ID fields exist or not
		 * @param crstimeZone
		 * @param record
		 * @return
		 */
		private static int checkIDFieldsExist(Integer crstimeZone, String[] record, boolean isTestJob) {
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
					return checkForOriginDest(record, crstimeZone, isTestJob);
				}else{
					return 1;
				}
			}else {
				return 1;
			}
		}

		/**
		 * Function to check whether Origin and Destination fields exist
		 * @param record
		 * @param crstimeZone
		 * @return code "0" for sanity passed, "1" for sanity failed and "2" for bad format
		 */
		private static int checkForOriginDest(String[] record, Integer crstimeZone, boolean isTestJob) {
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
				if (isTestJob) return 0; // if pipeline is for "test", do not proceed with further sanity check
				Integer cancelled = Integer.parseInt(record[47]);
				// Check for flights that are not Cancelled(1 = yes)
				return (cancelled.equals(0)) ? checkForArrAndDepTime(record, crstimeZone) : 0;
			}else {
				return 1;
			}
		}

		/**
		 * Function to check for time zone difference
		 * @param record
		 * @param crstimeZone
		 * @return code "0" for sanity passed, "1" for sanity failed and "2" for bad format
		 */ 
		private static int checkForArrAndDepTime(String[] record, Integer crstimeZone) {
			Integer arrTime = Integer.parseInt(record[41]);
			Integer depTime = Integer.parseInt(record[30]);
			Integer actualElapsedTime = Integer.parseInt(record[51]);
			Integer actualTimeZone = findActualTimeZone(arrTime,depTime,actualElapsedTime);
			Integer timeZoneDiff = actualTimeZone - crstimeZone;
			//arrTime -  depTime - actualElapsedTime - timeZone should be zero
			return (timeZoneDiff == 0) ? checkForArrDelay(record) : 1;
		}

		/**
		 * Function to check for ArrDelay Fields
		 * @param record
		 * @return code "0" for sanity passed, "1" for sanity failed and "2" for bad format
		 */
		private static int checkForArrDelay(String[] record) {
			Integer arrDelay = (int)Double.parseDouble(record[42]);
			Integer arrDelayMinutes = (int)Double.parseDouble(record[43]);
			Integer arrDel15 = (int)Double.parseDouble(record[44]);
			int finalReturnVal = 0;			
			if (arrDelay > 0){
				if (arrDelay.equals(arrDelayMinutes)){
					if (arrDelayMinutes >= 15){
						finalReturnVal = (arrDel15.equals(1)) ? 0: 1;
					}
				}
				else{
					finalReturnVal = 1;
				}
				
			}
			else{
				finalReturnVal = (arrDelayMinutes.equals(0)) ? 0 : 1;
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
			// Splitting the Arrival and Departure hours from CRS time
			Integer crsArrTimeHour = crsArrTime / 100;
			Integer crsDepTimeHour = crsDepTime / 100; 
			// Splitting the Arrival and Departure minutes from CRS time
			Integer crsArrTimeMin = crsArrTime % 100;
			Integer crsDepTimeMin = crsDepTime % 100;
			// Finding the time difference between CRS Arrival time and Departure times
			Integer crsHourDiff;
			if (crsArrTimeHour > crsDepTimeHour){
				crsHourDiff = crsArrTimeHour - crsDepTimeHour;
			}
			else if (crsArrTimeHour == crsDepTimeHour){
				if(crsArrTimeMin > crsDepTimeMin){
					crsHourDiff = crsArrTimeHour - crsDepTimeHour;    
				}
				else{					
					crsHourDiff = (24 - crsDepTimeHour) + crsArrTimeHour; // Time difference when the Arrival time is next day
				}
			}
			else{				
				crsHourDiff = (24 - crsDepTimeHour) + crsArrTimeHour; // Time difference when the Arrival time is next day
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
			// Splitting the Arrival and Departure hours from Actual time
			Integer arrTimeHour = arrTime / 100;
			Integer depTimeHour = depTime / 100;
			// Splitting the Arrival and Departure hours from Actual time
			Integer arrTimeMin = arrTime % 100;
			Integer depTimeMin = depTime % 100;                                
			// Finding the time difference between Actual Arrival time and Departure times
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