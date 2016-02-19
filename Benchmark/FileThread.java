import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;
import com.csvreader.CsvReader;

/**
 * @author Abhijeet Sharma
 * @version 1.0, 01/19/2016
 * Class for each thread submitted by Solution.java
 */
class FileThread implements Callable<FileThread>{
    private Thread t;
    private final String threadName;

    private String fileName;
    private HashMap<String, ArrayList<Double>> hm;
    private Integer saneCounter;
    private Integer insaneCounter;
    private Integer corruptCounter; 

    /**
     * Constructor
     * @param filename(threadname for thread) to be parsed.
     * 
     */
    public FileThread(String fileName){
        this.threadName = fileName;
        //System.out.println("Creating " + this.threadName);
        this.fileName = fileName;
        this.hm = new HashMap<String, ArrayList<Double>>();
        this.saneCounter = 0;
        this.insaneCounter = 0;
        this.corruptCounter = 1;
    }
    
    /**
     * getFileName
     * getter method for retrieving filename.
     *
     * @return  String filename.
     */
    public String getFileName(){
        return this.fileName;
    }
    
    /**
     * getPriceHashMap
     * getter method for retrieving price hashmap.
     *
     * @return  price hashmap.
     */
    public HashMap<String, ArrayList<Double>> getPriceHashMap(){
        return this.hm;
    }
    
    /**
     * updateHashMap
     * setter method for updating price hashmap.
     */
    public void updateHashMap(String carrierAndMonth, Double avgTicketPrice) 
                            throws IOException{
        if (this.hm.containsKey(carrierAndMonth)){
            this.hm.get(carrierAndMonth).add(avgTicketPrice);
        }
        else{
            ArrayList<Double> priceList = new ArrayList<Double>();
            priceList.add(avgTicketPrice);
            this.hm.put(carrierAndMonth, priceList);    
        }
    }
    
    /**
     * getSaneCounter
     * getter method for retrieving counter for records passing sanity check.
     */
    public Integer getSaneCounter(){
        return this.saneCounter;
    }

    /**
     * updateSaneCounter
     * setter method for updating counter for records passing sanity check.
     */
    public void updateSaneCounter(){
        this.saneCounter += 1;
    }
    
    /**
     * getInsaneCounter
     * getter method for retrieving counter for records failing sanity check.
     */
    public Integer getInsaneCounter(){
        return this.insaneCounter;
    }

    /**
     * updateInsaneCounter
     * setter method for updating counter for records failing sanity check.
     */
    public void updateInsaneCounter(){
        this.insaneCounter += 1;
    }
    
    /**
     * getCorruptCounter
     * getter method for retrieving counter for records with incorrect format.
     */
    public Integer getCorruptCounter(){
        return this.corruptCounter;
    }

    /**
     * updateCorruptCounter
     * setter method for updating counter for records with incorrect format.
     */
    public void updateCorruptCounter(){
        this.corruptCounter += 1;
    }
    
    /**
     * call
     * overrides Callable.call method
     * reads the given gzipped file, checks all records for format and sanity 
     * check and maintains a hashmap of average ticket price.
     *
     * @return  FileThread object.
     */
    @Override
    public FileThread call(){
        try{
            //System.out.println("Running " + this.threadName);
            String file = this.getFileName(); 
            //System.out.println("FILE NAME "+ file);
            FileInputStream fis = new FileInputStream(file);
            GZIPInputStream gis = new GZIPInputStream(fis);
            InputStreamReader isr = new InputStreamReader(gis);
            BufferedReader br = new BufferedReader(isr);
            CsvReader field = new CsvReader(br);

            Integer rowNum = 0;
            
            //Fetch Headers
            field.readHeaders();

            //Read CSV record-wise
            while (field.readRecord()) {
                rowNum += 1;
                FlightInfo flightInfo = this.getFlightInfo(field, rowNum);
                Integer code = this.isSane(flightInfo);
                if (code.equals(0)){
                    this.updateSaneCounter();
                        if (flightInfo.getAvgTicketPrice() != ""){
                            this.updateHashMap(flightInfo.getCarrier().concat(flightInfo.getMonth()), 
                            		Double.parseDouble(flightInfo.getAvgTicketPrice()));
                        }
                }
                else if (code.equals(1)){
                    this.updateInsaneCounter();    
                }
                else{
                    this.updateCorruptCounter();
                }
            }
            Thread.sleep(50);
        }
        catch(InterruptedException e){
            System.out.println("Thread " + this.threadName + " interrupted.");
        }
        catch(FileNotFoundException ef){
            System.out.println(ef.getClass().getName() + "\n" + ef.getMessage());
        }
        catch(IOException e){
            System.out.println(e.getClass().getName() + "\n" + e.getMessage());
        }
        //System.out.println("Thread " + this.threadName + " exiting.");
        return this;
    }
    
    /**
     * getFlightInfo
     * retrieves the Flight records from the given object and stores in another 
     * object.
     *
     * @params  CSVReader object a field of csv data.
     * @params  Integer rownumber of field.
     * @return  FlightInfo object.
     */
    public FlightInfo getFlightInfo(CsvReader field, Integer rowNum) 
                    throws IOException{
        FlightInfo flightInfo = new FlightInfo();
        flightInfo.setRowNum(rowNum);
        flightInfo.setCrsArrTime(field.get("CRS_ARR_TIME"));
        flightInfo.setCrsDepTime(field.get("CRS_DEP_TIME"));
        flightInfo.setCrsElapsedTime(field.get("CRS_ELAPSED_TIME"));
        flightInfo.setOriginAirportID(field.get("ORIGIN_AIRPORT_ID"));
        flightInfo.setOriginAirportSeqID(field.get("ORIGIN_AIRPORT_SEQ_ID"));
        flightInfo.setOriginCityMarketID(field.get("ORIGIN_CITY_MARKET_ID"));
        flightInfo.setOriginStateFips(field.get("ORIGIN_STATE_FIPS"));
        flightInfo.setOriginWAC(field.get("ORIGIN_WAC"));
        flightInfo.setDestAirportID(field.get("DEST_AIRPORT_ID"));
        flightInfo.setDestAirportSeqID(field.get("DEST_AIRPORT_SEQ_ID"));
        flightInfo.setDestCityMarketID(field.get("DEST_CITY_MARKET_ID"));
        flightInfo.setDestStateFips(field.get("DEST_STATE_FIPS"));
        flightInfo.setDestWAC(field.get("DEST_WAC"));
        flightInfo.setOrigin(field.get("ORIGIN"));
        flightInfo.setOriginCityName(field.get("ORIGIN_CITY_NAME"));
        flightInfo.setOriginStateABR(field.get("ORIGIN_STATE_ABR"));
        flightInfo.setOriginStateNM(field.get("ORIGIN_STATE_NM"));
        flightInfo.setDest(field.get("DEST"));
        flightInfo.setDestCityName(field.get("DEST_CITY_NAME"));
        flightInfo.setDestStateABR(field.get("DEST_STATE_ABR"));
        flightInfo.setDestStateNM(field.get("DEST_STATE_NM"));
        flightInfo.setArrTime(field.get("ARR_TIME"));
        flightInfo.setDepTime(field.get("DEP_TIME"));
        flightInfo.setActualElapsedTime(field.get("ACTUAL_ELAPSED_TIME"));
        flightInfo.setArrDelay(field.get("ARR_DELAY"));
        flightInfo.setArrDelayMinutes(field.get("ARR_DELAY_NEW"));
        flightInfo.setArrDel15(field.get("ARR_DEL15"));
        flightInfo.setCarrier(field.get("CARRIER"));
        flightInfo.setCancelled(field.get("CANCELLED"));
        flightInfo.setAvgTicketPrice(field.get("AVG_TICKET_PRICE"));
        flightInfo.setMonth(field.get("MONTH"));
        flightInfo.setQuarter(field.get("QUARTER"));
        flightInfo.setYear(field.get("YEAR"));

        return flightInfo;
    }
    
    /**
     * isSane
     * sanity check for flight data and returns sanity check code.
     * code 0 -> sanity passed
     * code 1 -> sanity failed
     * code 2 -> incorrect format
     * The sanity test is:
     * CRSArrTime and CRSDepTime should not be zero
     * timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
     * timeZone % 60 should be 0
     * AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
     * Origin, Destination,  CityName, State, StateName should not be empty
     * For flights that not Cancelled:
     * ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
     * if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
     * if ArrDelay < 0 then ArrDelayMinutes should be zero
     * if ArrDelayMinutes >= 15 then ArrDel15 should be true
     *
     * @params  FlightInfo object containing flight information.
     * @return  Integer sanity check code.
     *
     */
    public Integer isSane(FlightInfo flightInfo){

        // for debugging purposes
        String issue = "";
        try{
            Integer crsArrTime = Integer.parseInt(flightInfo.getCrsArrTime());
            Integer crsDepTime = Integer.parseInt(flightInfo.getCrsDepTime());
            Integer crsElapsedTime = Integer.parseInt(flightInfo.getCrsElapsedTime());
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
                    Integer originAirportID = Integer.parseInt(flightInfo.getOriginAirportID());
                    Integer originAirportSeqID = Integer.parseInt(flightInfo.getOriginAirportSeqID());
                    Integer originCityMarketID = Integer.parseInt(flightInfo.getOriginCityMarketID());
                    Integer originStateFips = Integer.parseInt(flightInfo.getOriginStateFips());
                    Integer originWAC = Integer.parseInt(flightInfo.getOriginWAC());
                    Integer destAirportID = Integer.parseInt(flightInfo.getDestAirportID());
                    Integer destAirportSeqID = Integer.parseInt(flightInfo.getDestAirportSeqID());
                    Integer destCityMarketID = Integer.parseInt(flightInfo.getDestCityMarketID());
                    Integer destStateFips = Integer.parseInt(flightInfo.getDestStateFips());
                    Integer destWAC = Integer.parseInt(flightInfo.getDestWAC());
                    // AirportID,  AirportSeqID, CityMarketID, StateFips, Wac 
                    // should be larger than 0
                    if ((originAirportID > 0) && (originAirportSeqID > 0) && 
                        (originCityMarketID > 0) && (originStateFips > 0) && 
                        (originWAC > 0) && (destAirportID > 0) && 
                        (destAirportSeqID > 0) && (destCityMarketID > 0) && 
                        (destStateFips > 0) && (destWAC > 0)){                
                        String origin = flightInfo.getOrigin();
                        String originCityName = flightInfo.getOriginCityName();
                        String originStateABR = flightInfo.getOriginStateABR();
                        String originStateNM = flightInfo.getOriginStateNM();
                        String dest = flightInfo.getDest();
                        String destCityName = flightInfo.getDestCityName();
                        String destStateABR = flightInfo.getDestStateABR();
                        String destStateNM = flightInfo.getDestStateNM();
                        // Origin, Destination,  CityName, State, StateName 
                        // should not be empty
                        if (!origin.equals("") && !originCityName.equals("") && 
                            !originStateABR.equals("") && !originStateNM.equals("") && 
                            !dest.equals("") && !destCityName.equals("") && 
                            !destStateABR.equals("") && !destStateNM.equals("")){                            
                            Integer cancelled = Integer.parseInt(flightInfo.getCancelled());
                            // For flights that are not Cancelled(1 = yes)
                            if(cancelled.equals(0)){
                                Integer arrTime = Integer.parseInt(flightInfo.getArrTime());
                                Integer depTime = Integer.parseInt(flightInfo.getDepTime());
                                Integer arrTimeHour = arrTime / 100;
                                Integer depTimeHour = depTime / 100;
                                Integer arrTimeMin = arrTime % 100;
                                Integer depTimeMin = depTime % 100;                                
                                Integer actualElapsedTime = Integer.parseInt(flightInfo.getActualElapsedTime());
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
                                    Integer arrDelay = (int)Double.parseDouble(flightInfo.getArrDelay());
                                    Integer arrDelayMinutes = (int)Double.parseDouble(flightInfo.getArrDelayMinutes());
                                    Integer arrDel15 = (int)Double.parseDouble(flightInfo.getArrDel15());
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
            return 2;
        }
        catch(Exception e){
            //System.out.format("Exception detected for Field no: %d.\n%s\nExiting with code: 0..\n", fieldNo, e);
            //System.out.format("Issue: %s", issue);
            return 2;
        }
        System.out.println("Issue: " + issue);
        return 1;
    }
}