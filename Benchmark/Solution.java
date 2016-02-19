import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.concurrent.*;

/**
 * @author Abhijeet Sharma
 * @version 1.0, 01/19/2016
 * 
 */
public class Solution{ 
    
    static int MINTHREADS = 1;
    static int MAXTHREADS = 5;

    /**
     * Main Method
     *
     * @param   args command-line arguments [-p for Parallel processing,-input=DIR for source directory,Mean/Median]
     *
     */
    public static void main(String[] args){
        try{
        	
            boolean doParallel = false;
            String dirPath = "";
            String findMeanOrMedian = null;
            try{
                // check for command-line arguments are proper
                if (args.length == 3 && args[0].equals("-p")){
                    doParallel = true;
                    dirPath = args[1].split("=")[1];
                    findMeanOrMedian = args[2];
                    
                }
                else if (args.length == 3 && args[1].equals("-p")){
                    doParallel = true;
                    dirPath = args[0].split("=")[1];
                    findMeanOrMedian = args[2];
                }
                else{
                    dirPath = args[0].split("=")[1];
                    findMeanOrMedian = args[1];
                }                
            }
            catch(ArrayIndexOutOfBoundsException e){
                System.err.println("Insufficient number of Arguments");
                System.exit(0);
            }
            
            File folder = new File(dirPath); 
            
            //System.out.println("FOLDER NAME "+folder);
            // if directory doesn't exist, quit.
            if(!folder.exists()){
                System.err.println(folder + " Directory doesn't exist!");
                System.exit(0);    
            }

            ExecutorService executor;
            if (doParallel){
                //To play nice with Java 1.8 code
                //executor = Executors.newWorkStealingPool();
                //To play nice with Java 1.7 code
                executor = Executors.newFixedThreadPool(MAXTHREADS);
            }
            else{
                executor = Executors.newFixedThreadPool(MINTHREADS);    
            }
    
            // call Map function
            List<Future<FileThread>> futures = map(folder, dirPath, executor);
    
            // call Reduce function
            ReduceResults<Integer, Integer, HashMap<String, ArrayList<Double>>> result = reduce(futures);
            
            // shutdown all threads(once processing is complete)
            executor.shutdown();
    
            // wait until all threads are finished
            while(!executor.isTerminated()){}

            // System.out.println("Finished all Threads");

            HashMap<String, ArrayList<Double>> allPriceHm = result.getC();

            HashMap<String, Double> mapValue = new HashMap<String, Double>();
            
            if (findMeanOrMedian.equals("mean")){
            	mapValue = getMeanPriceByCarrier(allPriceHm);
            }else if (findMeanOrMedian.equals("median")){
            	mapValue = getMedianPriceByCarrier(allPriceHm);
            }
      
            // sort the HashMap in ascending order and display in stdout
            TreeMap<Double, String> sortedHm = sortHashMap(mapValue);
            Writer writer = null;
            try{                
                if (doParallel){
                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output/output_multi_"+findMeanOrMedian+".txt"), "utf-8"));
                }
                else{
                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output/output_single_"+findMeanOrMedian+".txt"), "utf-8"));
                }
                

                for(Double menaOrMedianprice: sortedHm.keySet()){
                    String monthAndCarrier = sortedHm.get(menaOrMedianprice);
                    String carrier = monthAndCarrier.substring(0, 2);
                    String month = monthAndCarrier.substring(2,monthAndCarrier.length());
                    writer.write(month + " " + carrier + " "+ Math.round(menaOrMedianprice*100.0)/100.0 + "\n");  
                }
            }
            catch(Exception e){
                //e.getMessage();
            }
            finally{
                writer.close();
            }
        }
        catch(Exception e){
            System.out.println(e.getClass().getName() + "\n" + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * map
     * Runs multiple threads for processing data in each folder in given folder.
     * @param   folder File directory folder object.
     * @param   dirPath String directory path.
     * @param   executor ExecutorService object.
     *
     * @return  List object.
     */
    public static List<Future<FileThread>> map(File folder, String dirPath , 
                                                        ExecutorService executor){
        List<Future<FileThread>> futures = null;
        try{
            Set<Callable<FileThread>> callables = new HashSet<Callable<FileThread>>();
            for (final File fileEntry : folder.listFiles()){
                String fileName = fileEntry.getName();
                String completeFileName = dirPath + "/" + fileName;
                Callable<FileThread> fileThread = new FileThread(completeFileName);
                callables.add(fileThread);
            }
            futures = executor.invokeAll(callables);            
        }
        catch(InterruptedException e){
            System.out.println(e.getClass().getName() + "\n" + e.getMessage());
            e.printStackTrace();
        }
        return futures;
    }

    /**
     * reduce
     * collects results from multiple threads and returns final result to 
     * main program.
     * @param   futures List of Future object.
     * @return   ReduceResults object containing merged results of all threads.
     *
     */
    public static ReduceResults<Integer, Integer, HashMap<String, ArrayList<Double>>> 
                                reduce(List<Future<FileThread>> futures){        
        Integer totalK = 0;
        Integer totalF = 0;
        HashMap<String, ArrayList<Double>> allPriceHm = 
                                            new HashMap<String, ArrayList<Double>>();
        try{
            for (Future<FileThread> future: futures){
                FileThread ft = future.get();
                Integer K = ft.getInsaneCounter() + ft.getCorruptCounter();
                Integer F = ft.getSaneCounter();
                totalK += K;
                totalF += F;
                HashMap<String, ArrayList<Double>> priceHm = ft.getPriceHashMap();
                for(String key: priceHm.keySet()){
                    if (allPriceHm.containsKey(key)){
                        allPriceHm.get(key).addAll(priceHm.get(key));
                    }
                    else{
                        allPriceHm.put(key, priceHm.get(key));
                    }
                }
                /*priceHm.forEach((k, v) -> allPriceHm.merge(k, v, (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                }));*/
            }
        }
        catch(InterruptedException e){
            System.out.println(e.getClass().getName() + "\n" + e.getMessage());
            e.printStackTrace();
        }
        catch(ExecutionException e){
            System.out.println(e.getClass().getName() + "\n" + e.getMessage());
            e.printStackTrace();
        }
        return new ReduceResults<Integer, Integer, HashMap<String, ArrayList<Double>>>(totalK, totalF, allPriceHm);
    }
    
    /**
     * getMeanPriceByCarrier
     * calculates the mean price with respect to carrier name.
     * @param   allPriceHm HashMap containing list of average ticket prices.
     * @return  HashMap of mean ticket price with Carrier name as keys.
     *
     */
    public static HashMap<String, Double> getMeanPriceByCarrier(HashMap<String, ArrayList<Double>> allPriceHm){
        // calculate mean of average_ticket_price
        HashMap<String, Double> meanHm = new HashMap<String, Double>();
        for(String carrierAndMonth: allPriceHm.keySet()){
            ArrayList<Double> priceList = allPriceHm.get(carrierAndMonth);            
            Double meanPrice = calculateMeanPrice(priceList);
            meanHm.put(carrierAndMonth, meanPrice);
        }
        return meanHm;
    }

    /**
     * calculateMeanPrice
     * returns the mean of a given arrayList.
     * @param   priceList An ArrayList of Double elements.
     * @return  Mean of given ArrayList.
     *
     */
    public static Double calculateMeanPrice(ArrayList<Double> priceList){
        Double sum = 0.0;
        if (!priceList.isEmpty()){
            for(Double price: priceList){
                sum += price;
            }
            return sum/priceList.size();
        }
        return sum;
    }
    
    /**
     * getMedianPriceByCarrier
     * calculates the median price with respect to carrier name.
     * @param   allPriceHm Hashmap containing list of average ticket prices.
     * @return  Hashmap of median ticket price with Carrier name as keys.
     *
     */
    public static HashMap<String, Double> getMedianPriceByCarrier(HashMap<String, ArrayList<Double>> allPriceHm){
        // calculate median of average_ticket_price
        HashMap<String, Double> medianHm = new HashMap<String, Double>();
        for(String carrierAndMonth: allPriceHm.keySet()){
            ArrayList<Double> priceList = allPriceHm.get(carrierAndMonth);            
            Double medianPrice = calculateMedianPrice(priceList);
            medianHm.put(carrierAndMonth, medianPrice);
        }
        return medianHm;
    }
    
    /**
     * calculateMedianPrice
     * returns the median of a given arrayList.
     * @param   priceList An ArrayList of Double elements.
     * @return  Median of given ArrayList.
     *
     */
    public static Double calculateMedianPrice(ArrayList<Double> priceList){
        try{
            Collections.sort(priceList);
            if(priceList.size() % 2 == 1){
                return priceList.get((priceList.size() + 1) / 2 - 1);
            }
            else{
                double lower = priceList.get(priceList.size()/2 - 1);
                double upper = priceList.get(priceList.size()/2);
                return (lower + upper) / 2.0;
            }
        }
        catch(Exception e){
            System.out.println(e.getClass().getName() + "\n" + e.getMessage());
            return 0.0;
        }
    }   

    /**
     * sortHashMap
     * returns a TreeMap sorted by value from the given HashMap.
     * @param   unsortedMap An Unsorted HashMap of double values.
     * @return  A sorted TreeMap with values as "keys" and carrier names as "values".
     *
     */    
    public static TreeMap<Double, String> sortHashMap(HashMap<String, Double> unsortedMap){
        // Function returns a sorted TreeMap given an unsorted HashMap as input
        TreeMap<Double, String> sortedMap = new TreeMap<Double, String>();
        for (Map.Entry<String, Double> entry : unsortedMap.entrySet()) {
            sortedMap.put(entry.getValue(), entry.getKey());
        }
        return sortedMap;
    }
}

/**
 * Generic version of ReduceResults class.(Since multiple returns not possible
 * in JAVA)
 * @param A, B, C the type of the value being boxed.
 */
class ReduceResults<A, B, C>{
    private A _1;
    private B _2;
    private C _3;
    
    /**
     * Constructor
     * @param initializes the values.
     * 
     */
    ReduceResults(A _1, B _2, C _3){
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
    }
    public A getA(){
        return this._1;
    }
    public B getB(){
        return this._2;
    }
    public C getC(){
        return this._3;
    }
}