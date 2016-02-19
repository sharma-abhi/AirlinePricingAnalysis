	/**
	 * @author: Abhijeet Sharma, Afan Ahmad Khan
	 * @version: 1.0
	 * @released: January 29, 2016
	 * The program takes a hadoop directory path as input containing multiple
	 * gzipped csv files and creates a plot displaying the average ticket prices
	 * of all airlines per month(restricted for airlines active in 2015).
	 */
	import java.io.IOException;
	import java.util.StringTokenizer;
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
	import org.apache.hadoop.mapreduce.RecordWriter;

		public class CarrierCount{

			// Mapper class in the Map-Reduce model
			public static class TokenizerMapper
				extends Mapper<Object, Text, Text, Text>{
					
					// Initialize the text variable
					private Text word = new Text();
		            
					//Map to have a list of average price, month and year and set it with the corresponding key
					public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
						// This variable stores the data of each record in the form of an array
						String[] itr = null;
						// To clean the values and splitting them with ','
						itr = value.toString().replaceAll("\"","").split(",");
						// Initializing the text variable
		                Text one = null;
		                // Check whether the flight is sane
						Integer code = isSane(itr);
						// Code 0 is returned when the flight is sane
						if (code.equals(0)){
							try{
								// Logic to check whether Average Price is present for the particular flight or not
								if (itr.length == 112 && itr[111] != ""){
									// Code block to debug errors
		                            if (itr[0] == ""){
		                                System.out.println("Error in Mapper ........" + Arrays.toString(itr));
		                            }
		                            else{
		                            	// Text which contains a concatenated value of Average Price, Month and Year
			                            one = new Text(itr[111] + ";" + itr[2] + ";" + itr[0]);
			                            word.set(itr[8]);
			                            context.write(word, one);
		                            }
								}
							}
							// Debugger to check errors
							catch(ArrayIndexOutOfBoundsException e){
								//System.out.println("Error.." + value.toString() + " " + itr.length);
							}
						}
						else if(code.equals(1)){
	                        // for debugging purposes
						   //System.out.println("Error.. " + itr + " ");
						}
						else{
	                        // for debugging purposes
						   //System.out.println("Error.." + itr + " ");
						}
					}
				}

				// Reduce class of Map-Reduce model
			public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
				// variable to segment the output to various files
		        /*private MultipleOutputs mos;

		        public void setup(Context context) {
		        	mos = new MultipleOutputs(context);
		        }*/

		        // Reduce method to merge the values of a particular carrier
		        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		        	// Boolean value to check whether the flight is active in 2015 or not.
					Boolean isActive = false;
					// Map to put the month and the sum of average prices
		            HashMap<Integer, ArrayList<Double>> monthPriceMap= new HashMap<Integer, ArrayList<Double>>();
		            //HashMap<String, Integer> yearMap= new HashMap<String, Integer>();
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
		                    System.out.println("Something is wrong.....");
		                    System.out.println(Arrays.toString(arr));
		                    System.out.println(val.toString());
		                }
		                // Set the isActive boolean if the year is 2015
		                if (year.equals(2015)){
		                    isActive = true;
		                }
		                ArrayList<Double> priceList = new ArrayList<Double>();
		                if (monthPriceMap.containsKey(month)){
						    priceList = monthPriceMap.get(month);
		                }
		                // Aggregate all the price values into one list
		                priceList.add(ticketP);
		                monthPriceMap.put(month, priceList);
					}
					// Code to calculate the sum of all average price for a particular carrier if it is from the year 2015
	                for (Integer mapKey: monthPriceMap.keySet()){
	                    ArrayList<Double> priceList = monthPriceMap.get(mapKey);
	                    Double sum = 0.0;
	                    Integer count = priceList.size();
	                    for (Double ticketPrice: priceList){
	                        sum += ticketPrice;
	                    }
	                    Double avg = sum/count;
	                    context.write(key, new Text(avg.toString() + "\t"+ mapKey.toString() + "\t" + count.toString() + "\t" + isActive.toString()));                   
	                }
				}

				// Clean up method to close the multiple segmented files
		        /*@Override
		        public void cleanup(Context context) throws IOException, InterruptedException {
		            mos.close();
		        }    */
			}


			// Main method to start Mapper and Reducer
			public static void main(String[] args)	 throws Exception{
				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf, "carrier count");
				job.setJarByClass(CarrierCount.class);
				job.setMapperClass(TokenizerMapper.class);
				//job.setCombinerClass(IntSumReducer.class);
				job.setReducerClass(IntSumReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        //MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, LongWritable.class, Text.class);
				System.exit(job.waitForCompletion(true) ? 0: 1);
			}

			// Code block to check whether the flight is sane
			public static Integer isSane(String[] record){
		        // for debugging purposes
		        String issue = "";
		        try{
		            Integer crsArrTime = Integer.parseInt(record[42]);
		            Integer crsDepTime = Integer.parseInt(record[31]);
		            Integer crsElapsedTime = Integer.parseInt(record[52]);
		            // crsArrTime and CRSDepTime should not be zero
		            if (crsArrTime != 0 && crsDepTime != 0){
		                Integer crsArrTimeHour = crsArrTime / 100;
		                Integer crsDepTimeHour = crsDepTime / 100;  
		                Integer crsArrTimeMin = crsArrTime % 100;
		                Integer crsDepTimeMin = crsDepTime % 100;
		                Integer crsHourDiff;
		                if (crsArrTimeHour > crsDepTimeHour){
		                    crsHourDiff = crsArrTimeHour - crsDepTimeHour;
		                }
		                else if (crsArrTimeHour == crsDepTimeHour){
		                    if(crsArrTimeMin > crsDepTimeMin){
		                        crsHourDiff = crsArrTimeHour - crsDepTimeHour;    
		                    }
		                    else{
		                        crsHourDiff = (24 - crsDepTimeHour) + crsArrTimeHour;                               
		                    }
		                }
		                else{
		                    crsHourDiff = (24 - crsDepTimeHour) + crsArrTimeHour;
		                }                
		                // timeZone = crsArrTimeMin - crsDepTimeMin - crsElapsedTime;              
		                Integer timeZone = ((crsHourDiff * 60) + (crsArrTimeMin - crsDepTimeMin)) - crsElapsedTime;
		                // timeZone % 60 should be 0
		                if (timeZone % 60 == 0){
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
		                            // For flights that are not Cancelled(1 = yes)
		                            if(cancelled.equals(0)){
		                                Integer arrTime = Integer.parseInt(record[43]);
		                                Integer depTime = Integer.parseInt(record[32]);
		                                Integer arrTimeHour = arrTime / 100;
		                                Integer depTimeHour = depTime / 100;
		                                Integer arrTimeMin = arrTime % 100;
		                                Integer depTimeMin = depTime % 100;                                
		                                Integer actualElapsedTime = Integer.parseInt(record[53]);
		                                Integer hourDiff;

		                                if (arrTimeHour > depTimeHour){
		                                    hourDiff = arrTimeHour - depTimeHour;
		                                }
		                                else if (arrTimeHour == depTimeHour){
		                                    if(arrTimeMin > depTimeMin){
		                                        hourDiff = arrTimeHour - depTimeHour;    
		                                    }
		                                    else{
		                                        hourDiff = (24 - depTimeHour) + arrTimeHour;       
		                                    }
		                                }
		                                else{
		                                    hourDiff = (24 - depTimeHour) + arrTimeHour;   
		                                }
		                                Integer tempVal = ((hourDiff * 60) + (arrTimeMin - depTimeMin)) - 
		                                                    actualElapsedTime - timeZone;
		                                
		                                //arrTime -  depTime - actualElapsedTime - timeZone should be zero
		                                if (tempVal == 0){
		                                    Integer arrDelay = (int)Double.parseDouble(record[44]);
		                                    Integer arrDelayMinutes = (int)Double.parseDouble(record[45]);
		                                    Integer arrDel15 = (int)Double.parseDouble(record[46]);
		                                    // if ArrDelay > 0 then ArrDelay should be 
		                                    // equal to ArrDelayMinutes
		                                    if (arrDelay > 0){                                        
		                                        if (arrDelay.equals(arrDelayMinutes)){
		                                            // end of sanity check for flights that are not cancelled and
		                                            // arrDelay > 0 and arrDelay == arrDelayMinutes .
		                                            return 0;   
		                                        }
		                                    }
		                                    // if ArrDelay < 0 then ArrDelayMinutes 
		                                    // should be zero
		                                    else if(arrDelay < 0){
		                                        if (arrDelayMinutes.equals(0)){
		                                            // end of sanity check for flights that are not cancelled and
		                                            // arrDelay < 0 and arrDelayMinutes equals zero.
		                                            return 0;
		                                        }
		                                    }
		                                    // if ArrDelayMinutes >= 15 then 
		                                    // ArrDel15 should be 1(true)
		                                    if (arrDelayMinutes >= 15){
		                                        if (arrDel15.equals(1)){
		                                            // end of sanity check for flights that are not cancelled and
		                                            // arrDelayMinute >= 15 and arrDel15 == 1.
		                                            return 0;
		                                        }
		                                    }
		                                    else{
		                                        // end of sanity check for flights that are not cancelled and
		                                        // arrDelayMinute < 15.
		                                        return 0;
		                                    }
		                                }
		                                else{
		                                    System.out.format("arrTime: %d ", arrTime);
		                                    System.out.format("arrTimeHour: %d ", arrTimeHour);
		                                    System.out.format("arrTimeMin: %d\n", arrTimeMin);
		                                    
		                                    System.out.format("depTime: %d ", depTime);
		                                    System.out.format("depTimeHour: %d ", depTimeHour);
		                                    System.out.format("depTimeMin: %d\n", depTimeMin);
		                                    System.out.format("hourDiff: %d\n", hourDiff);

		                                    System.out.format("actualElapsedTime: %d ", actualElapsedTime);
		                                    System.out.format("timeZone: %d ", timeZone);
		                                    System.out.format("tempVal: %d\n", tempVal);
		                                    
		                                    System.out.format("crsarrTime: %d ", crsArrTime);
		                                    System.out.format("crsArrTimeHour: %d ", crsArrTimeHour);
		                                    System.out.format("crsArrTimeMin: %d\n", crsArrTimeMin);
		                                    
		                                    System.out.format("crsDepTime: %d ", crsDepTime);
		                                    System.out.format("crsDepTimeHour: %d ", crsDepTimeHour);
		                                    System.out.format("crsDepTimeMin: %d\n", crsDepTimeMin);
		                                    
		                                    System.out.format("crsHourDiff: %d\n",crsHourDiff);
		                                    System.out.format("crsElapsedTime: %d\n", crsElapsedTime);

		                                    issue = "arrTime - depTime - actualElapsedTime - timeZone is " +
		                                            "not equal to zero";
		                                }                                    
		                            }
		                            else{
		                                // end of sanity check for flights not cancelled.
		                                return 0;
		                            }
		                        }
		                        else{
		                            issue = "Origin, Destination,  CityName, State or StateName is empty";
		                        }
		                    }
		                    else{
		                        issue = "AirportID,  AirportSeqID, CityMarketID, StateFips or Wac " +
		                                "is not larger than zero";    
		                    }
		                }
		                else{
		                    System.out.println("crsArrTime: " + crsArrTime);
		                    System.out.println("crsDepTime" + crsDepTime);
		                    System.out.println("crsElapsedTime" + crsElapsedTime);
		                    issue = "timeZone % 60 is not zero";
		                }
		            }
		            else{
		                issue = "crsArrTime or CRSDepTime is zero";
		            }
		        }
		        catch(NumberFormatException e){
		            //System.out.println(e.getClass().getName() + "\n" + e.getMessage());
		            //System.out.println(Arrays.toString(record));
		            return 2;
		        }
		        catch(Exception e){
		            //System.out.format("Exception detected for Field no: %d.\n%s\nExiting with code: 0..\n",  e);
		            //System.out.format("Issue: %s", issue);
		            return 2;
		        }
		        System.out.println("Issue: " + issue);
		        return 1;
		    }
		}
