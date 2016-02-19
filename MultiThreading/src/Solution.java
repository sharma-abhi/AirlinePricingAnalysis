/*
 * <SOURCE_HEADER>
 *
 * <NAME>
 * Solution
 * </NAME>
 *
 * <SOURCE>
 * $Revision: 1.0 $
 * $Date: 2016/01/19 $
 * </SOURCE>
 *
 * The program takes a directory as input containing gzipped csv files and 
 * counts number of lines which are in incorrect format or fails sanity test (K)
 * and number of lines which are in correct format and passes sanity test(F)
 * It also calculates mean and median of avg_ticket_price by carrier and displays it in
 * console in ascending order. 
 *
 * <PERFORMANCE>
 * $average execution time: 37.101s 
 * </PERFORMANCE>
 *
 * <COPYRIGHT>
 * The following source code is protected under all standard copyright laws.
 * </COPYRIGHT>
 *
 * </SOURCE_HEADER>
 */

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

    /**
     * Main Method
     *
     * @param   args command-line arguments [-p for Parallel processing, -input=DIR for source directory]
     *
     */
    public static void main(String[] args){
        try{
            boolean doParallel = false;
            String dirPath = "";
            try{
                // check for command-line arguments
                if (args.length == 2 && args[0].equals("-p")){
                    doParallel = true;
                    dirPath = args[1].split("=")[1];
                    
                }
                else if (args.length == 2 && args[1].equals("-p")){
                    doParallel = true;
                    dirPath = args[0].split("=")[1];
                    
                }
                else{
                    dirPath = args[0].split("=")[1];
                }                
            }
            catch(ArrayIndexOutOfBoundsException e){
                System.err.println("Insufficient number of Arguments");
                System.exit(0);
            }
            
            File folder = new File(dirPath);  
            // if directory doesn't exist, quit.
            if(!folder.exists()){
                System.err.println(folder + " Directory doesn't exist!");
                System.exit(0);    
            }
            ExecutorService executor;
            if (doParallel){
                executor = Executors.newWorkStealingPool();
            }
            else{
                executor = Executors.newSingleThreadExecutor();    
            }
    
            // call Map function
            List<Future<FileThread>> futures = map(folder, dirPath, executor);
    
            // call Reduce function
            ReduceResults<Integer, Integer, HashMap<String, ArrayList<Double>>> result = reduce(futures);
            
            // shutdown all threads(once processsing is complete)
            executor.shutdown();
    
            // wait until all threads are finished
            while(!executor.isTerminated()){}

            // System.out.println("Finished all Threads");
            Integer totalK = result.getA();
            Integer totalF = result.getB();
            HashMap<String, ArrayList<Double>> allPriceHm = result.getC();

            System.out.println("K: " + totalK);
            System.out.println("F: " + totalF);

            HashMap<String, Double> meanHm = getMeanPriceByCarrier(allPriceHm);
            HashMap<String, Double> medianHm = getMedianPriceByCarrier(allPriceHm);
            
            // sort the HashMap in ascending order and display in stdout
            TreeMap<Double, String> sortedHm = sortHashMap(meanHm);
            for(Double name: sortedHm.keySet()){
                String meanValue = name.toString();
                String carrierName = sortedHm.get(name);
                Double medianValue = medianHm.get(carrierName);
                System.out.println(carrierName + " " + meanValue + " " + medianValue + " " +"\n");  
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
                String completeFileName = dirPath + "//" + fileName;
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
    public static ReduceResults<Integer, 
                                Integer, 
                                HashMap<String, ArrayList<Double>>> 
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
                priceHm.forEach((k, v) -> allPriceHm.merge(k, v, (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                }));
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
     * @param   priceHm HashMap containing list of average ticket prices.
     * @return  HashMap of mean ticket price with Carrier name as keys.
     *
     */
    public static HashMap<String, Double> getMeanPriceByCarrier(HashMap<String, 
                                                    ArrayList<Double>> priceHm){
        // calculate mean of average_ticket_price
        HashMap<String, Double> meanHm = new HashMap<String, Double>();
        for(String name: priceHm.keySet()){
            String key = name.toString();
            ArrayList<Double> priceList = priceHm.get(name);            
            Double meanPrice = calculateMeanPrice(priceList);
            meanHm.put(key, meanPrice);
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
     * @param   priceHm Hashmap containing list of average ticket prices.
     * @return  Hashmap of median ticket price with Carrier name as keys.
     *
     */
    public static HashMap<String, Double> getMedianPriceByCarrier(HashMap<String, ArrayList<Double>> priceHm){
        // calculate median of average_ticket_price
        HashMap<String, Double> medianHm = new HashMap<String, Double>();
        for(String name: priceHm.keySet()){
            String key = name.toString();
            ArrayList<Double> priceList = priceHm.get(name);            
            Double medianPrice = calculateMedianPrice(priceList);
            medianHm.put(key, medianPrice);
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