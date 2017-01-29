import java.util.*;

/*
Multithreaded class that implements Coarse locking. All the threads share a single accumulation hashmap.
Each thread updates this shared structure. When a thread is accessing the shared hashmap, the whole structure is
locked. The lock is released only after the thread has done accessing the hashmap and other threads won't be able to
access this hasmap when it is locked. Once all the threads are done, the average is calculated for the whole output.
Since this is a thread class, it implements the Runnable interface and has the Run method.
 */

public class CoarseLock implements Runnable{
    List<String> data = new ArrayList<String>();
    HashMap<String,List<Double>> coarseLockAccumulator= new HashMap<String,List<Double>>();
    HashMap<String,Double> coarseLockOutput= new HashMap<String,Double>();

    public CoarseLock(List<String> data, HashMap<String,List<Double>> coarseLockAccumulator, HashMap<String,Double> coarseLockOutput){
        this.data = data;
        this.coarseLockAccumulator = coarseLockAccumulator;
        this.coarseLockOutput = coarseLockOutput;
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
                // the entire data structure is locked here by the current thread that executes this method
                synchronized (coarseLockAccumulator) {
                    fibonacci(17);
                    if (coarseLockAccumulator.get(temp.get(0)) == null) {
                        List<Double> t = new ArrayList<Double>();
                        t.add(Double.parseDouble(temp.get(3)));
                        t.add(1.0);
                        coarseLockAccumulator.put(temp.get(0), t);
                    } else {
                        Double tem = Double.parseDouble(temp.get(3));
                        if(tem!=null){
                            Double sum = coarseLockAccumulator.get(temp.get(0)).get(0) + tem;
                            Double count = coarseLockAccumulator.get(temp.get(0)).get(1) + 1.0;
                            List<Double> s = new ArrayList<Double>();
                            s.add(sum);
                            s.add(count);
                            coarseLockAccumulator.put(temp.get(0), s);
                        }
                    }
                } // lock is released here
            }
        }
    }
    // this method is called from the main class after both threads have competed and fully updated the shared accumulation structure
    public void calculateAverage(HashMap<String,List<Double>> coarseLockAccumulator){
        for (Map.Entry<String, List<Double>> entry : coarseLockAccumulator.entrySet()) {
            Double avg = entry.getValue().get(0)/entry.getValue().get(1);
            coarseLockOutput.put(entry.getKey(), avg);
        }
    }

    public void run() {
        try {
            accumulate();
        } catch (NullPointerException|ConcurrentModificationException e) {
            System.err.format("Exception: %s%n", e);
        }
    }
}