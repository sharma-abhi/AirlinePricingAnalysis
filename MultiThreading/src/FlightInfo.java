/*
 * <SOURCE_HEADER>
 *
 * <NAME>
 * FlightInfo
 * </NAME>
 *
 * <SOURCE>
 * $Revision: 1.0 $
 * $Date: 2016/01/19 $
 * </SOURCE>
 *
 * <COPYRIGHT>
 * The following source code is protected under all standard copyright laws.
 * </COPYRIGHT>
 *
 * </SOURCE_HEADER>
 */

import java.io.*;
import java.util.*;

/**
 * @author Abhijeet Sharma
 * @version 1.0, 01/19/2016
 * Class for storing flight information.
 */
class FlightInfo{
	private Integer rowNum;
	private String crsArrTime;
	private String crsDepTime;
	private String crsElapsedTime;
	private String originAirportID;
    private String originAirportSeqID;
    private String originCityMarketID;
    private String originStateFips;
    private String originWAC;
    private String destAirportID;
    private String destAirportSeqID;
    private String destCityMarketID;
    private String destStateFips;
    private String destWAC;
    private String origin;
    private String originCityName;
    private String originStateABR;
    private String originStateNM;
    private String dest;
    private String destCityName;
    private String destStateABR;
    private String destStateNM;
    private String arrTime;
	private String depTime;
	private String actualElapsedTime;
	private String arrDelay;
    private String arrDelayMinutes;
	private String arrDel15;
	private String carrier;
	private String cancelled;
	private String avgTicketPrice;
	private String month;
	private String quarter;
	private String year;
	
	public Integer getRowNum(){
		return this.rowNum;
	}
	
	public void setRowNum(Integer rowNum){
		this.rowNum = rowNum;
	}

	public String getCrsArrTime(){
		return this.crsArrTime;
	}
	
	public void setCrsArrTime(String crsArrTime){
		this.crsArrTime = crsArrTime;
	}

	public String getCrsDepTime(){
		return this.crsDepTime;
	}
	
	public void setCrsDepTime(String crsDepTime){
		this.crsDepTime = crsDepTime;
	}

	public String getCrsElapsedTime(){
		return this.crsElapsedTime;
	}
	
	public void setCrsElapsedTime(String crsElapsedTime){
		this.crsElapsedTime = crsElapsedTime;
	}

	public String getOriginAirportID(){
		return this.originAirportID;
	}
	
	public void setOriginAirportID(String originAirportID){
		this.originAirportID = originAirportID;
	}

	public String getOriginAirportSeqID(){
		return this.originAirportSeqID;
	}
	
	public void setOriginAirportSeqID(String originAirportSeqID){
		this.originAirportSeqID = originAirportSeqID;
	}

	public String getOriginCityMarketID(){
		return this.originCityMarketID;
	}
	
	public void setOriginCityMarketID(String originCityMarketID){
		this.originCityMarketID = originCityMarketID;
	}

	public String getOriginStateFips(){
		return this.originStateFips;
	}
	
	public void setOriginStateFips(String originStateFips){
		this.originStateFips = originStateFips;
	}

	public String getOriginWAC(){
		return this.originWAC;
	}
	
	public void setOriginWAC(String originWAC){
		this.originWAC = originWAC;
	}

	public String getDestAirportID(){
		return this.destAirportID;
	}
	
	public void setDestAirportID(String destAirportID){
		this.destAirportID = destAirportID;
	}

	public String getDestAirportSeqID(){
		return this.destAirportSeqID;
	}
	
	public void setDestAirportSeqID(String destAirportSeqID){
		this.destAirportSeqID = destAirportSeqID;
	}

	public String getDestCityMarketID(){
		return this.destCityMarketID;
	}
	
	public void setDestCityMarketID(String destCityMarketID){
		this.destCityMarketID = destCityMarketID;
	}

	public String getDestStateFips(){
		return this.destStateFips;
	}
	
	public void setDestStateFips(String destStateFips){
		this.destStateFips = destStateFips;
	}

	public String getDestWAC(){
		return this.destWAC;
	}
	
	public void setDestWAC(String destWAC){
		this.destWAC = destWAC;
	}

	public String getOrigin(){
		return this.origin;
	}
	public void setOrigin(String origin){
		this.origin = origin;
	}

	public String getOriginCityName(){
		return this.originCityName;
	}
	public void setOriginCityName(String originCityName){
		this.originCityName = originCityName;
	}

	public String getOriginStateABR(){
		return this.originStateABR;
	}
	public void setOriginStateABR(String originStateABR){
		this.originStateABR = originStateABR;
	}

	public String getOriginStateNM(){
		return this.originStateNM;
	}
	public void setOriginStateNM(String originStateNM){
		this.originStateNM = originStateNM;
	}
	public String getDest(){
		return this.dest;
	}
	public void setDest(String dest){
		this.dest = dest;
	}

	public String getDestCityName(){
		return this.destCityName;
	}

	public void setDestCityName(String destCityName){
		this.destCityName = destCityName;
	}

	public String getDestStateABR(){
		return this.destStateABR;
	}

	public void setDestStateABR(String destStateABR){
		this.destStateABR = destStateABR;
	}

	public String getDestStateNM(){
		return this.destStateNM;
	}

	public void setDestStateNM(String destStateNM){
		this.destStateNM = destStateNM;
	}

	public String getArrTime(){
		return this.arrTime;
	}
	
	public void setArrTime(String arrTime){
		this.arrTime = arrTime;
	}

	public String getDepTime(){
		return this.depTime;
	}
	
	public void setDepTime(String depTime){
		this.depTime = depTime;
	}

	public String getActualElapsedTime(){
		return this.actualElapsedTime;
	}
	
	public void setActualElapsedTime(String actualElapsedTime){
		this.actualElapsedTime = actualElapsedTime;
	}

	public String getArrDelay(){
		return this.arrDelay;
	}
	
	public void setArrDelay(String arrDelay){
		this.arrDelay = arrDelay;
	}

	public String getArrDelayMinutes(){
		return this.arrDelayMinutes;
	}
	
	public void setArrDelayMinutes(String arrDelayMinutes){
		this.arrDelayMinutes = arrDelayMinutes;
	}

	public String getArrDel15(){
		return this.arrDel15;
	}
	
	public void setArrDel15(String arrDel15){
		this.arrDel15 = arrDel15;
	}

	public String getCarrier(){
		return this.carrier;
	}
	
	public void setCarrier(String carrier){
		this.carrier = carrier;
	}

	public String getCancelled(){
		return this.cancelled;
	}
	
	public void setCancelled(String cancelled){
		this.cancelled = cancelled;
	}

	public String getAvgTicketPrice(){
		return this.avgTicketPrice;
	}
	
	public void setAvgTicketPrice(String avgTicketPrice){
		this.avgTicketPrice = avgTicketPrice;
	}

	public String getMonth(){
		return this.month;
	}
	public void setMonth(String month){
		this.month = month;
	}
	
	public String getQuarter(){
		return this.quarter;
	}
	
	public void setQuarter(String quarter){
		this.quarter = quarter;
	}

	public String getYear(){
		return this.year;
	}
	
	public void setYear(String year){
		this.year = year;
	}
}