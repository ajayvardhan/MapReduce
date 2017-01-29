import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*
The main class where all shared variables are stored, initial file is loaded and parsed, threads are created and executed.
Once the accumulation structores are populated, the averages are calculated. Each execution is timed and in the end,
average, minimum and maximum of 10 executions of each program is printed.
*/

public class AverageTemperature {
    String fileName = null;
    public AverageTemperature(String fileName){
        this.fileName = fileName;
    }

    // readFile() the loader routine that parses the input file and return a list of strings
    public List<String> readFile(){
        String line = null;
        List<String> outputList = new ArrayList<>(); // output list of strings
        try{
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            while ((line = br.readLine()) != null){
                outputList.add(line);
            }
            br.close();
        } catch(IOException e){
            System.err.format("IOException: %s%n", e);
        }
        return outputList;
    }

    //finalCalculation() takes in the the list of time taken by each programs and gives out the average, minimum and maximum times
    //input is a lit of times in milliseconds and output is a list of average, min and max times
    public static List<Long> finalCalculation(List<Long> lst){
        List<Long> output = new ArrayList<Long>();
        long avg = 0;
        long min = lst.get(0);
        long max = lst.get(0);
        for (long l: lst){
            avg += l;
            if (l<min){
                min = l;
            }
            if (l>max){
                max = l;
            }
        }
        output.add(avg/lst.size());
        output.add(min);
        output.add(max);
        return output;
    }

    public static void main(String[] args) throws InterruptedException {
        String fileName = "/home/ajay/Documents/MR/Assignment 1/1912.csv";
        HashMap<String,Double> sequentialOutput= new HashMap<String,Double>(); // output data structure for sequential execution
        List<Long> sequentialTimes= new ArrayList<Long>(); // list to store the time taken by each execution of the sequential programe

        // following variables are the shared accumulator hashmaps, output hashmaps and list to store the execution times for the respective programs
        HashMap<String,List<Double>> noLockAccumulator= new HashMap<String,List<Double>>();
        HashMap<String,Double> noLockOutput= new HashMap<String,Double>();
        List<Long> noLockTimes= new ArrayList<Long>();
        HashMap<String,List<Double>> coarseLockAccumulator= new HashMap<String,List<Double>>();
        HashMap<String,Double> coarseLockOutput= new HashMap<String,Double>();
        List<Long> coarseLockTimes= new ArrayList<Long>();
        // concurrent hashmap is used to handle granular locking of the values. This data structure allows us to have a synchronized
        // execution of multithreaded programs without having to lock the whole data structure but just locking the key that is
        // currently being accessed.
        ConcurrentHashMap<String,List<Double>> fineLockAccumulator = new ConcurrentHashMap<String,List<Double>>();
        ConcurrentHashMap<String,Double> fineLockOutput = new ConcurrentHashMap<String,Double>();
        List<Long> fineLockTimes= new ArrayList<Long>();
        //two separate accumulators for no sharing execution
        HashMap<String,List<Double>> noSharingAccumulator1= new HashMap<String,List<Double>>();
        HashMap<String,List<Double>> noSharingAccumulator2= new HashMap<String,List<Double>>();
        HashMap<String,Double> noSharingOutput= new HashMap<String,Double>();
        List<Long> noSharingTimes= new ArrayList<Long>();
        AverageTemperature at = new AverageTemperature(fileName); // load and parse file
        List<String> temperatures = at.readFile();
        // split list of strings into two parts
        List<String> temperatures1 = temperatures.subList(0,(temperatures.size()/2));
        List<String> temperatures2 = temperatures.subList((temperatures.size()/2),temperatures.size());
        Sequential s = new Sequential(temperatures);
        for(int i=0;i<10;i++){
            Long startTime = System.currentTimeMillis();
            sequentialOutput = s.calculateAverage();
            Long endTime = System.currentTimeMillis();
            sequentialTimes.add(endTime-startTime);
        }
        for (int i=0;i<10;i+=1) {
            Long startTime = System.currentTimeMillis();
            NoLock noLock1 = new NoLock(temperatures1, noLockAccumulator, noLockOutput);
            NoLock noLock2 = new NoLock(temperatures2, noLockAccumulator, noLockOutput);
            Thread noLockThread1 = new Thread(noLock1);
            Thread noLockThread2 = new Thread(noLock2);
            noLockThread1.start();
            noLockThread2.start();
            noLockThread1.join();
            noLockThread2.join();
            noLock1.calculateAverage(noLockAccumulator);
            Long endTime = System.currentTimeMillis();
            noLockTimes.add(endTime-startTime);
        }
        for (int i=0;i<10;i+=1) {
            Long startTime = System.currentTimeMillis();
            CoarseLock coarseLock1 = new CoarseLock(temperatures1, coarseLockAccumulator, coarseLockOutput);
            CoarseLock coarseLock2 = new CoarseLock(temperatures2, coarseLockAccumulator, coarseLockOutput);
            Thread coarseLockThread1 = new Thread(coarseLock1);
            Thread coarseLockThread2 = new Thread(coarseLock2);
            coarseLockThread1.start();
            coarseLockThread2.start();
            coarseLockThread1.join();
            coarseLockThread2.join();
            coarseLock1.calculateAverage(coarseLockAccumulator);
            Long endTime = System.currentTimeMillis();
            coarseLockTimes.add(endTime-startTime);
        }
        for (int i=0;i<10;i+=1) {
            Long startTime = System.currentTimeMillis();
            FineLock fineLock1 = new FineLock(temperatures1, fineLockAccumulator, fineLockOutput);
            FineLock fineLock2 = new FineLock(temperatures2, fineLockAccumulator, fineLockOutput);
            Thread fineLockThread1 = new Thread(fineLock1);
            Thread fineLockThread2 = new Thread(fineLock2);
            fineLockThread1.start();
            fineLockThread2.start();
            fineLockThread1.join();
            fineLockThread2.join();
            fineLock1.calculateAverage(fineLockAccumulator);
            Long endTime = System.currentTimeMillis();
            fineLockTimes.add(endTime-startTime);
        }
        for (int i=0;i<10;i+=1) {
            Long startTime = System.currentTimeMillis();
            NoSharing noSharing1 = new NoSharing(temperatures1, noSharingAccumulator1);
            NoSharing noSharing2 = new NoSharing(temperatures2, noSharingAccumulator2);
            Thread noSharingThread1 = new Thread(noSharing1);
            Thread noSharingThread2 = new Thread(noSharing2);
            noSharingThread1.start();
            noSharingThread2.start();
            noSharingThread1.join();
            noSharingThread2.join();
            for (Map.Entry<String, List<Double>> entry : noSharingAccumulator1.entrySet()) { // merge the two output lists and calculate average
                if(noSharingAccumulator2.containsKey(entry.getKey())){
                    List<Double> temp = new ArrayList<Double>();
                    temp.add(entry.getValue().get(0)+noSharingAccumulator2.get(entry.getKey()).get(0));
                    temp.add(entry.getValue().get(1)+noSharingAccumulator2.get(entry.getKey()).get(1));
                    noSharingAccumulator2.put(entry.getKey(),temp);
                }
                else{
                    noSharingAccumulator2.put(entry.getKey(),entry.getValue());
                }
            }
            for (Map.Entry<String, List<Double>> entry : noSharingAccumulator2.entrySet()) {
                Double avg = entry.getValue().get(0)/entry.getValue().get(1);
                noSharingOutput.put(entry.getKey(), avg);
            }
            Long endTime = System.currentTimeMillis();
            noSharingTimes.add(endTime-startTime);
        }
        sequentialTimes = finalCalculation(sequentialTimes);
        System.out.println("Average Sequential Time " + Long.toString(sequentialTimes.get(0)));
        System.out.println("Minimum Sequential Time " + Long.toString(sequentialTimes.get(1)));
        System.out.println("Maximum Sequential Time " + Long.toString(sequentialTimes.get(2)));
        noLockTimes = finalCalculation(noLockTimes);
        System.out.println("Average No Lock Time " + Long.toString(noLockTimes.get(0)));
        System.out.println("Minimum No Lock Time " + Long.toString(noLockTimes.get(1)));
        System.out.println("Maximum No Lock Time " + Long.toString(noLockTimes.get(2)));
        coarseLockTimes = finalCalculation(coarseLockTimes);
        System.out.println("Average Coarse Lock Time " + Long.toString(coarseLockTimes.get(0)));
        System.out.println("Minimum Coarse Lock Time " + Long.toString(coarseLockTimes.get(1)));
        System.out.println("Maximum Coarse Lock Time " + Long.toString(coarseLockTimes.get(2)));
        fineLockTimes = finalCalculation(fineLockTimes);
        System.out.println("Average Fine Lock Time " + Long.toString(fineLockTimes.get(0)));
        System.out.println("Minimum Fine Lock Time " + Long.toString(fineLockTimes.get(1)));
        System.out.println("Maximum Fine Lock Time " + Long.toString(fineLockTimes.get(2)));
        noSharingTimes = finalCalculation(noSharingTimes);
        System.out.println("Average No Sharing Time " + Long.toString(noSharingTimes.get(0)));
        System.out.println("Minimum No Sharing Time " + Long.toString(noSharingTimes.get(1)));
        System.out.println("Maximum No Sharing Time " + Long.toString(noSharingTimes.get(2)));
    }
}