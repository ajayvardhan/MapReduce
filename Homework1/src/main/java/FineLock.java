import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/*
Multithreaded class that implements fine locking. All the threads share a single accumulation Concurrent hashmap
that provides an in-built key level granular synchronization. Each thread updates this shared structure and once all
the threads are done, the average is calculated for the whole output. Since this is a thread class, it implements
the Runnable interface and has the Run method.
 */
public class FineLock implements Runnable{
    List<String> data = new ArrayList<String>();
    ConcurrentHashMap<String,List<Double>> fineLockAccumulator= new ConcurrentHashMap<String,List<Double>>();
    ConcurrentHashMap<String,Double> fineLockOutput= new ConcurrentHashMap<String,Double>();

    public FineLock(List<String> data, ConcurrentHashMap<String,List<Double>> fineLockAccumulator, ConcurrentHashMap<String,Double> fineLockOutput){
        this.data = data;
        this.fineLockAccumulator = fineLockAccumulator;
        this.fineLockOutput = fineLockOutput;
    }

    public int fibonacci(int n){
        if (n == 1)
            return 0;
        if (n == 2)
            return 1;
        return fibonacci(n - 1) + fibonacci(n - 2);
    }

    public void accumulate(){
        for (String temperature : data) {
            List<String> temp = new ArrayList<String>();
            temp = Arrays.asList(temperature.split(","));
            if (temp.get(2).equals("TMAX")) {
                // there is no need for synchronization here since the concurrent hashmap takes care of the locking by itself
                if (fineLockAccumulator.get(temp.get(0)) == null) {
                    List<Double> t = new ArrayList<Double>();
                    t.add(Double.parseDouble(temp.get(3)));
                    t.add(1.0);
                    fineLockAccumulator.put(temp.get(0), t);
                } else {
                    Double tem = Double.parseDouble(temp.get(3));
                    if(tem!=null){
                        Double sum = fineLockAccumulator.get(temp.get(0)).get(0) + tem;
                        Double count = fineLockAccumulator.get(temp.get(0)).get(1) + 1.0;
                        fibonacci(17);
                        List<Double> s = new ArrayList<Double>();
                        s.add(sum);
                        s.add(count);
                        fineLockAccumulator.put(temp.get(0), s);
                    }
                }
            }
        }
    }

    // this method is called from the main class after both threads have competed and fully updated the shared accumulation structure
    public void calculateAverage(ConcurrentHashMap<String,List<Double>> fineLockAccumulator){
        for (Map.Entry<String, List<Double>> entry : fineLockAccumulator.entrySet()) {
            Double avg = entry.getValue().get(0)/entry.getValue().get(1);
            fineLockOutput.put(entry.getKey(), avg);
        }
    }

    public void run() {
        try {
            accumulate();
        } catch (Exception e) {
            System.err.format("Exception: %s%n", e);
        }
    }
}