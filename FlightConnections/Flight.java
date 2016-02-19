
public class Flight {
	
	private String location;
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	private String depTime;
	private String arrTime;
	private String crsDepTime;
	private String crsArrTime;
	private String cancelled;
	public String getDepTime() {
		return depTime;
	}
	public void setDepTime(String depTime) {
		this.depTime = depTime;
	}
	public String getArrTime() {
		return arrTime;
	}
	public void setArrTime(String arrTime) {
		this.arrTime = arrTime;
	}
	public String getCrsDepTime() {
		return crsDepTime;
	}
	public void setCrsDepTime(String crsDepTime) {
		this.crsDepTime = crsDepTime;
	}
	public String getCrsArrTime() {
		return crsArrTime;
	}
	public void setCrsArrTime(String crsArrTime) {
		this.crsArrTime = crsArrTime;
	}
	public String getCancelled() {
		return cancelled;
	}
	public void setCancelled(String cancelled) {
		this.cancelled = cancelled;
	}
}
