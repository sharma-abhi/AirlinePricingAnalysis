import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * <h1>Hadoop Reducer</h1>
 * The PreProcessReducer will find missed connections between 
 * given origin and destination flights.
 * 
 * @author: Deepen, Abhijeet, Akshay, Afan
 * @version: 2.0
 */

public class PreProcessReducer extends Reducer<Text, Text, Text, Text> {
	/**
	 * Loops over Iterable<Text> to build a list of FlightInfo objects and compute connections
	 */
	// List of popular airports fetched from prof's piazza post "@276"
	static String[] popularAirports = { "ATL", "ORD", "DFW", "LAX", "DEN", "IAH", "PHX", 
			"SFO", "CLT", "DTW", "MSP", "LAS", "MCO", "EWR", 
			"JFK", "LGA", "BOS", "SLC", "SEA", "BWI", "MIA", 
			"MDW", "PHL", "SAN", "FLL", "TPA", "DCA", "IAD", 
	"HOU"};

	HashSet<String> requests = new HashSet<String>();

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {	
		String requestFolder = context.getConfiguration().get("requestFolder");
		FileSystem fileSystem = FileSystem.get(URI.create(requestFolder), context.getConfiguration());
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path(requestFolder+"/04req10k.csv.gz")); 	
		BufferedReader br=new BufferedReader(new InputStreamReader(new GZIPInputStream(fsDataInputStream)));
		String line = br.readLine();
		while(line != null) {
			String cols[] = line.split(",");
			String joinCols = merge(cols);
			requests.add(joinCols);
			line = br.readLine();
		}
		//System.out.println("Requests Size :: "+requests.size());
	}
	
	private String merge(String[] cols) {	
		String str = cols[0]+"_"+cols[1]+"_"+cols[2]+"_"+cols[3]+"_"+cols[4];
		return str;
	}


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean isTest = context.getConfiguration().get("mode").equals("test");
		ArrayList<FlightInfo> flightInfoList = new ArrayList<>();
		for(Text featuresVal : values) {
			String[] features = featuresVal.toString().split("\t");
			FlightInfo fInfo = new FlightInfo();			
			fInfo.setDayOfWeek(Integer.parseInt(features[0]));
			fInfo.setMonth(Integer.parseInt(features[1]));
			fInfo.setDistanceGroup(Integer.parseInt(features[2]));
			fInfo.setDayOfMonth(Integer.parseInt(features[3]));
			fInfo.setCrsTime(features[4]);							// Set Destination/Arrival CRS Time in String
			fInfo.setCrsTimeLong(Long.parseLong(features[5]));		// Set Destination/Arrival CRS Time in long
			fInfo.setLocation(features[6]);							// Set Destination Airport for departing flight, Origin Airport for arriving flight
			fInfo.setFlightType(features[7].charAt(0));				// 'd' for departing, 'a' for arrival Flight
			if(!isTest) {
				fInfo.setActualTime(Long.parseLong(features[8]));	// Set Destination/Arrival CRS Time (only for train data)
			} else {
				fInfo.setStartEndTime(Long.parseLong(features[8]));
				fInfo.setYear(Integer.parseInt(features[9]));
				fInfo.setFlightNumber(Integer.parseInt(features[10]));				
			}
			flightInfoList.add(fInfo);			
		}		
		Collections.sort(flightInfoList, new TimeComparator());		// Sort the collection of FlightInfo objects according to time
		// Compute the connections, missed connections and no connections between all FlightInfo objects
		getAllConnection(flightInfoList, context, key, isTest);
	}

	/**
	 * Writes the engineered features from the given list of FlightInfo objects along with missed connection flag.
	 * @param flightInfoList ArrayList of FlightInfo objects
	 * @param context 
	 * @param key 
	 * @param isTest 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	private void getAllConnection(ArrayList<FlightInfo> flightInfoList, Reducer<Text, Text, Text, Text>.Context context, Text key, boolean isTest) throws IOException, InterruptedException {
		try {
			int flightIndex = 0;
			for (FlightInfo arrivalFlight : flightInfoList) {
				flightIndex ++;
				// Look for a connection for this arriving flight
				if(arrivalFlight.getFlightType() == 'a') {										// verify flight is indeed an arriving flight.
					int departFlightIndex = flightIndex;
					boolean isConnectionPossible = true;
					// List is sorted.If no connection found for current -> no connection will be found for rest of list elements.
					while(departFlightIndex < flightInfoList.size() && isConnectionPossible) { 
						FlightInfo departFlight = flightInfoList.get(departFlightIndex);						
						if (departFlight.getFlightType() == 'd' && !arrivalFlight.getLocation().equalsIgnoreCase(departFlight.getLocation())) {								// verify flight is indeed a departing flight
							long diffScheduledTime = TimeUnit.MILLISECONDS.toMinutes(
									departFlight.getCrsTimeLong() - arrivalFlight.getCrsTimeLong());
							// Time difference should be more than (or equal to ) 30 minutes and less than (or equal to ) 1 hour
							if (diffScheduledTime <= 60) {
								if (diffScheduledTime >= 30){
									if(isTest) {
										generateData(departFlight, arrivalFlight, context, -1, key, diffScheduledTime); //end of parsing for test data
									} 
									else {
										long diffActualTime = TimeUnit.MILLISECONDS.toMinutes(
												departFlight.getActualTime() - arrivalFlight.getActualTime());
										if(diffActualTime < 30) {
											generateData(departFlight, arrivalFlight, context, 1, key, diffScheduledTime); // missed
										}
										else {
											generateData(departFlight, arrivalFlight, context, 0, key, diffScheduledTime); // not missed
										}
									}
								}
							} else isConnectionPossible = false;	// no connection
						}
						departFlightIndex++;
					}
				}
			}
		} 
		catch (NullPointerException  | NumberFormatException e)  {
			e.printStackTrace();
		}
	}

	private void generateData(FlightInfo departFlight, FlightInfo arrivalFlight,
			Reducer<Text, Text, Text, Text>.Context context, int label, Text key, long scheduledLayoverDuration) throws IOException, InterruptedException {


		String engineeredFeatures = isPopularAirport(arrivalFlight.getLocation()) + "\t" + 		// starting origin ariport
				//isPopularAirport(departFlight.getLocation()) + "\t" + 		// final destination airport
				//isPopularAirport(key.toString().split("\t")[1]) + "\t" + 	// intermediate airport
				scheduledLayoverDuration + "\t" + 							// scheduled layover Duration
				arrivalFlight.getDayOfWeek() + "\t" + 						// Day of Week [1-7]
				arrivalFlight.getMonth() + "\t" + 							// Month [1-12]
				//binDayToWeek(arrivalFlight.getDayOfMonth()) + "\t" +		// Week No [0-4]
				binScheduledTime(arrivalFlight.getCrsTime()) + "\t" +		// Binned Scheduled time [0-24]
				arrivalFlight.getDistanceGroup();	


		// Distance Group
		if (label == -1){			// test data
			if (isRequested(arrivalFlight.getYear(), arrivalFlight.getMonth(), arrivalFlight.getDayOfMonth(), 
					arrivalFlight.getLocation(), departFlight.getLocation(), requests)) {
				String testKey = 
						arrivalFlight.getYear() + "\t" + 
								arrivalFlight.getMonth() + "\t"  + 
								arrivalFlight.getDayOfMonth() + "\t" + 
								arrivalFlight.getLocation() + "\t" + 
								departFlight.getLocation() + "\t" + 
								arrivalFlight.getFlightNumber() + "\t" + 
								departFlight.getFlightNumber();

				long difference = departFlight.getStartEndTime() - arrivalFlight.getStartEndTime();
				long duration = TimeUnit.MILLISECONDS.toMinutes(difference);
				context.write(new Text(testKey), new Text(engineeredFeatures + "\t" + duration));
			} 

		}
		else{						// train data
			context.write(new Text(""), new Text(engineeredFeatures + "\t" + label));
		}
	}

	public static boolean isRequested (Integer year, Integer month, Integer day, String ori, String dest, HashSet<String> requests) {

		String key = year+"_"+month+"_"+day+"_"+ori+"_"+dest;		
		return requests.contains(key);
	}

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

	public static String binDayToWeek(Integer dayOfMonth){
		/**
		 * Bins day of a month to its corresponding week number(division by 7).
		 * @param	dayOfMonth day as a String
		 * @return	Week number of the day
		 */
		Integer week = dayOfMonth/7;
		return week.toString();
	}

	public static String binScheduledTime(String scheduledTime){
		/**
		 * Bins time into 24 bins (division by 100).
		 * @param	scheduledTime time in hhmm format as String.
		 * @return	Bin of the given given time 
		 */
		Integer time = Integer.parseInt(scheduledTime)/100;
		return time.toString();
	}
}

/**
 * FlightInfo class is used to store relevant information of a Flight object for computing connections
 * based on arrival and departure times.
 */
class FlightInfo {
	Integer dayOfWeek;
	Integer month;	
	Integer dayOfMonth;
	String crsTime;
	Long crsTimeLong;
	Long actualTime;
	String location;
	char flightType;
	Integer distanceGroup;
	Integer year;
	Integer flightNumber;
	Long startEndTime;


	public Long getStartEndTime() {
		return startEndTime;
	}

	public void setStartEndTime(Long startEndTime) {
		this.startEndTime = startEndTime;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getFlightNumber() {
		return flightNumber;
	}

	public void setFlightNumber(Integer flightNumber) {
		this.flightNumber = flightNumber;
	}

	public FlightInfo() {

	}

	public Integer getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(Integer dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public Integer getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(Integer dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public String getCrsTime() {
		return crsTime;
	}

	public void setCrsTime(String crsTime) {
		this.crsTime = crsTime;
	}

	public Long getCrsTimeLong() {
		return crsTimeLong;
	}

	public void setCrsTimeLong(Long crsTimeLong) {
		this.crsTimeLong = crsTimeLong;
	}

	public Long getActualTime() {
		return actualTime;
	}

	public void setActualTime(Long actualTime) {
		this.actualTime = actualTime;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public char getFlightType() {
		return flightType;
	}

	public void setFlightType(char flightType) {
		this.flightType = flightType;
	}

	public Integer getDistanceGroup() {
		return distanceGroup;
	}

	public void setDistanceGroup(Integer distanceGroup) {
		this.distanceGroup = distanceGroup;
	}
}

/**
 * TimeComparator class implements logic to compare two FlightInfo objects.
 */
class TimeComparator implements Comparator<FlightInfo> {
	@Override
	public int compare(FlightInfo flight1, FlightInfo flight2) {
		return flight1.getCrsTimeLong().compareTo(flight2.getCrsTimeLong());
	}
}
