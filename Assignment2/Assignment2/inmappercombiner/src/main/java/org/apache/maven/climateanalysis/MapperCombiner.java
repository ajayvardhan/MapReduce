package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/*
In mapper combiner aggregates the input, aggregates values amd makes a single emit with the aggregated values for each station ID.
The values are aggregated in the global hashmap with the station ID as the key, temperate label, temperature and count
 as values. This hashmap is aggregated by iterating through the values. Later the hashmap is iterated and
 the values are emitted for each Station ID in the format <Text,TextArrayWritable>
<StationID, [Temperature Label, Temperature, Count]>.
*/

public class MapperCombiner extends Mapper<Object, Text, Text, TextArrayWritable> {
    private Text stationID = new Text();
    private Text[] values = new Text[3];
    private Text tmaxlabel = new Text("TMAX");
    private Text tminlabel = new Text("TMIN");
    private HashMap<String,HashMap<String,Double[]>> results; // global hashmap to store the station ID and aggregated values

    // setup function initializes the global hashmap
    public void setup(Context context) throws IOException, InterruptedException{
        results = new HashMap<String, HashMap<String,Double[]>>();
    }

    // map function aggregates the values in the hashmap
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] temp = new String[10];
        temp = value.toString().split(",");
        if ((temp[2].equals("TMAX") || temp[2].equals("TMIN") && !temp[3].equals(""))){
            HashMap<String,Double[]> h= new HashMap<String, Double[]>();
            Double[] d = new Double[2];
            d[0] = Double.parseDouble(temp[3]);
            d[1] = 1.0;
            if (results.containsKey(temp[0])){
                if (results.get(temp[0]).containsKey(temp[2])){
                    results.get(temp[0]).get(temp[2])[0] += Double.parseDouble(temp[3]);
                    results.get(temp[0]).get(temp[2])[1] += 1.0;
                }
                else{
                    results.get(temp[0]).put(temp[2],d);
                }
            }
            else{
                h.put(temp[2],d);
                results.put(temp[0], h);
            }
        }
    }

    // cleanup iterates through the hashmap and emits the values for each station ID
    public void cleanup(Context context) throws IOException, InterruptedException{
        System.out.println(results.keySet());
        for (String s : results.keySet()){
            stationID.set(s);
            for (String tempType : results.get(s).keySet()){
                Text sum = new Text(Double.toString(results.get(s).get(tempType)[0]));
                Text count = new Text(Double.toString(results.get(s).get(tempType)[1]));
                values[0] = new Text(tempType);
                values[1] = sum;
                values[2] = count;
                context.write(stationID, new TextArrayWritable(values));
            }
        }
    }
}