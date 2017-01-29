import java.util.*;
/*
Multithreaded class that don't share anything. Each thread works with its own set of input and
in the end the output of all threads are merged in the main class. Since this is a thread class,
it implements the Runnable interface and has the Run method.
 */
public class NoSharing implements Runnable{
    List<String> data = new ArrayList<String>();
    HashMap<String,List<Double>> noSharingAccumulator= new HashMap<String,List<Double>>(); // local hashmap for the shared accumulation structure

    public NoSharing(List<String> data, HashMap<String,List<Double>> noSharingAccumulator){
        this.data = data;
        this.noSharingAccumulator = noSharingAccumulator;
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
                if (noSharingAccumulator.get(temp.get(0)) == null) {
                    List<Double> t = new ArrayList<Double>();
                    t.add(Double.parseDouble(temp.get(3)));
                    t.add(1.0);
                    noSharingAccumulator.put(temp.get(0), t);
                } else {
                    Double tem = Double.parseDouble(temp.get(3));
                    if(tem!=null){
                        Double sum = noSharingAccumulator.get(temp.get(0)).get(0) + tem;
                        Double count = noSharingAccumulator.get(temp.get(0)).get(1) + 1.0;
                        List<Double> s = new ArrayList<Double>();
                        s.add(sum);
                        s.add(count);
                        noSharingAccumulator.put(temp.get(0), s);
                    }
                }
            }
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