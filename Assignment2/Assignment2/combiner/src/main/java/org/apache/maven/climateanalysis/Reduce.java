package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
Reducer class receives the output from the mapper as <Text, list of TextArrayWritable>
<StationID, list of [Temperature Label, Temperature, Count]>. Each reduce call gets the list of values
for a single station ID. The mean is calculated by iterating though the values and the
<Station ID, meanMin, meanMax> is emitted.
 */

public class Reduce extends Reducer<Text,TextArrayWritable,NullWritable,Text> {
    public void reduce(Text key, Iterable<TextArrayWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        double minSum, maxSum, minCount, maxCount;
        minSum = maxSum = minCount = maxCount = 0.0;
        for (TextArrayWritable val : values) {
            String[] temp = val.toStrings();
            if(temp[0].equals("TMAX")){
                maxSum+=Double.parseDouble(temp[1]);
                maxCount+=Double.parseDouble(temp[2]);
            }
            else if(temp[0].equals("TMIN")){
                minSum+=Double.parseDouble(temp[1]);
                minCount+=Double.parseDouble(temp[2]);
            }
        }
        String meanMin, meanMax;
        if(minCount==0){
            meanMin = "0";
        }
        else{
            meanMin = Double.toString(minSum/minCount);
        }
        if(maxCount==0){
            meanMax = "0";
        }
        else{
            meanMax = Double.toString(maxSum/maxCount);
        }
        String s = key.toString() + "," + meanMin + "," + meanMax;
        Text result = new Text();
        result.set(s);
        context.write(NullWritable.get(), result);
    }
}