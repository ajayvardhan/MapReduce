import java.util.*;

/*
Multithreaded class that doesn't have any locking. All the threads share a single accumulation hashmap.
Each thread updates this shared structure. Since there are no locks, both threads can update  this structure simultaneously and
the output can be incorrect because of this. Once all the threads are done, the average is calculated for the whole output.
Since this is a thread class, it implements the Runnable interface and has the Run method.
 */

public class NoLock implements Runnable{
    List<String> data = new ArrayList<String>();
    HashMap<String,List<Double>> noLockAccumulator= new HashMap<String,List<Double>>();
    HashMap<String,Double> noLockOutput= new HashMap<String,Double>();

    public NoLock(List<String> data, HashMap<String,List<Double>> noLockAccumulator, HashMap<String,Double> noLockOutput){
        this.data = data;
        this.noLockAccumulator = noLockAccumulator;
        this.noLockOutput = noLockOutput;
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
                fibonacci(17);
                if (noLockAccumulator.get(temp.get(0)) == null) {
                    List<Double> t = new ArrayList<Double>();
                    t.add(Double.parseDouble(temp.get(3)));
                    t.add(1.0);
                    noLockAccumulator.put(temp.get(0), t);
                } else {
                    Double tem = Double.parseDouble(temp.get(3));
                    if(tem!=null){
                        Double sum = noLockAccumulator.get(temp.get(0)).get(0) + tem;
                        Double count = noLockAccumulator.get(temp.get(0)).get(1) + 1.0;
                        List<Double> s = new ArrayList<Double>();
                        s.add(sum);
                        s.add(count);
                        noLockAccumulator.put(temp.get(0), s);
                    }
                }
            }
        }
    }
    // this method is called from the main class after both threads have competed and fully updated the shared accumulation structure
    public void calculateAverage(HashMap<String,List<Double>> noLockAccumulator){
        for (Map.Entry<String,List<Double>> entry : noLockAccumulator.entrySet()) {
            Double avg = entry.getValue().get(0)/entry.getValue().get(1);
            noLockOutput.put(entry.getKey(), avg);
        }
    }

    // since there is no locking, the program can throw a ConcurrentModificationException occasionally. That is being handled here.
    public void run() throws ConcurrentModificationException, NullPointerException {
        try {
            accumulate();
        } catch (ConcurrentModificationException|NullPointerException e) {
            System.err.format("Exception: %s%n", e);
        }
    }
}