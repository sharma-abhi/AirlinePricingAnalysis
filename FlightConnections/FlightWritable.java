//
//public class PriceTime {
//	
//	private String price;
//	private String time;
//	public String getPrice() {
//		return price;
//	}
//	public void setPrice(String itr) {
//		this.price = itr;
//	}
//	public String getTime() {
//		return time;
//	}
//	public void setTime(String itr) {
//		this.time = itr;
//	}
//}

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


/**
 * @author Unmesha SreeVeni U.B
 *
 */
public class FlightWritable implements Writable {
    private Text origin;
    private Text destn;
    
    public  FlightWritable() {
        set(new Text(), new Text());
    }

//    public  PriceWritable() {
//        set(origin, destn);
//    }
    public  FlightWritable(String origin, String destn) {
        set(new Text(origin), new Text(destn));
    }
    public void set(Text origin2, Text destn2) {
        this.origin = origin2;
        this.destn = destn2;
    }
    public Text getOrigin() {
        return origin;
    }
    public Text getDestn() {
        return destn;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        origin.write(out);
        destn.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        origin.readFields(in);
        destn.readFields(in);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return origin.hashCode();
    }
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
//    @Override
//    public boolean equals(Object obj) {
//        if (this == obj) {
//            return true;
//        }
//        if (obj == null) {
//            return false;
//        }
//        if (!(obj instanceof PriceWritable)) {
//            return false;
//        }
//        PriceWritable other = (PriceWritable) obj;
//        if (Double.doubleToLongBits(origin) != Double
//                .doubleToLongBits(other.origin)) {
//            return false;
//        }
//        if (Double.doubleToLongBits(destn) != Double
//                .doubleToLongBits(other.destn)) {
//            return false;
//        }
//        return true;
//    }
//    @Override
//    public String toString() {
//        return origin + "," + destn;
//    }
}
