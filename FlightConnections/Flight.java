/**
 * This class acts as a POJO class for Flight data. This class contains the 
 * fields that are sent as values from the Mapper to Reducer class in the MR Job.
 * @author Afan, Abhijeet
 * Version 1.0
 *
 */
public class Flight {
	private String key;
	private String crsTime;
	private String actualTime;
	private String cancelled;
	private String location;
	private String type;
	
	public String getActualTime() {
		return actualTime;
	}
	public void setActualTime(String actualTime) {
		this.actualTime = actualTime;
	}
	public String getCrsTime() {
		return crsTime;
	}
	public void setCrsTime(String crsTime) {
		this.crsTime = crsTime;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getCancelled() {
		return cancelled;
	}
	public void setCancelled(String cancelled) {
		this.cancelled = cancelled;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	/**
	 * Overriding the toString method to return the appended String values
	 */
	public String toString() {
		return crsTime + "\t" + actualTime + "\t" + cancelled + "\t" + location + "\t" + type;
	}
}
