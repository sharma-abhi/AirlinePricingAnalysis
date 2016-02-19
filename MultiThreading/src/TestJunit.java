import java.util.*;
import org.junit.Test;
import junit.framework.*;
import static org.junit.Assert.assertEquals;

public class TestJunit extends TestCase {
   protected HashMap<String, ArrayList<Double>> priceHm;
   ArrayList<Double> al;

   protected void setUp(){
      this.priceHm = new HashMap<String, ArrayList<Double>>();
      this.al = new ArrayList<Double>();
      this.al.add(20.5);
      this.al.add(10.5);
      this.al.add(30.5);
      this.priceHm.put("AA", al);
      this.al = new ArrayList<Double>();
      this.al.add(1.5);
      this.al.add(3.3);
      this.al.add(2.2);
      this.al.add(4.5);
      this.priceHm.put("BB", al);
   }
   
   @Test
   public void testSortAvgPrice(){
      HashMap<String, Double> priceHm = new HashMap<String, Double>();  
      priceHm.put("AA", 1.1);
      priceHm.put("BB", 1.3);
      priceHm.put("DD", 1.2);
      TreeMap<Double, String>  sortedHm = new TreeMap<Double, String>();
      sortedHm.put(1.1, "AA");
      sortedHm.put(1.2, "DD");
      sortedHm.put(1.3, "BB");
      assertEquals(sortedHm, Solution.sortHashMap(priceHm));
   }
   
   @Test
   public void testGetMeanPriceByCarrier(){
      HashMap<String, Double> meanHm = new HashMap<String, Double>();
      meanHm.put("AA", 20.5);
      meanHm.put("BB", 2.875);
      assertEquals(meanHm, Solution.getMeanPriceByCarrier(this.priceHm));
   }

   @Test
   public void testCalculateMeanPrice(){
      assertEquals(2.875, Solution.calculateMeanPrice(this.al));
   }

   @Test
   public void testGetMedianPriceByCarrier(){
      HashMap<String, Double> medianHm = new HashMap<String, Double>();
      medianHm.put("AA", 20.5);
      medianHm.put("BB", 2.75);
      assertEquals(medianHm, Solution.getMedianPriceByCarrier(this.priceHm));
   }
   
   @Test
   public void testCalculateMedianPrice(){
      assertEquals(2.75, Solution.calculateMedianPrice(this.al));
   }
}

