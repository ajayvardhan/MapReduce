import java.util.*;

/*
This is a sequential execution class. There are no multiple threads. The input is parsed sequentially
and the average is calculated one by one.
 */

public class Sequential {

    List<String> data = new ArrayList<String>();

    public Sequential(List<String> data){
        this.data = data;
    }

    public int fibonacci(int n){
        if (n == 1)
            return 0;
        if (n == 2)
            return 1;
        return fibonacci(n - 1) + fibonacci(n - 2);
    }

    public HashMap<String,Double> calculateAverage(){
        HashMap<String,List<Double>> output= new HashMap<String,List<Double>>();
        HashMap<String,Double> finalOutput= new HashMap<String,Double>();
        for(String temperature: data){
            List<String> temp = new ArrayList<String>();
            temp = Arrays.asList(temperature.split(","));
            if (temp.get(2).equals("TMAX")){
                fibonacci(17);
                if (output.get(temp.get(0)) == null){
                    List<Double> t = new ArrayList<Double>();
                    t.add(Double.parseDouble(temp.get(3)));
                    t.add(1.0);
                    output.put(temp.get(0), t);
                }
                else{
                    Double tem = Double.parseDouble(temp.get(3));
                    if(tem!=null){
                        Double sum = output.get(temp.get(0)).get(0) + tem;
                        Double count = output.get(temp.get(0)).get(1) + 1.0;
                        List<Double> s = new ArrayList<Double>();
                        s.add(sum);
                        s.add(count);
                        output.put(temp.get(0), s);
                    }
                }
            }
        }
        for (Map.Entry<String, List<Double>> entry : output.entrySet()) {
            Double avg = entry.getValue().get(0)/entry.getValue().get(1);
            finalOutput.put(entry.getKey(), avg);
        }
        return finalOutput;
    }
}