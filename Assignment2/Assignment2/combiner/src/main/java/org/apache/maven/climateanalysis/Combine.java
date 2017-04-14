package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/*
Combiner class for the Program using custom combiner. Mapper sends the output <Text,TextArrayWritable>
<StationID, [Temperature Label, Temperature, Count]> to the combiner class. Combiner class aggregates
the temperature and count for this particular station ID and emits the same output as the mapper.
<Text,TextArrayWritable> <StationID, [Temperature Label, Temperature, Count]>.
*/
public class Combine extends Reducer<Text,TextArrayWritable,Text,TextArrayWritable> {
    public void reduce(Text key, Iterable<TextArrayWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        // variables to accumulate the min and max sum and count.
        double minSum, maxSum, minCount, maxCount;
        minSum = maxSum = minCount = maxCount = 0.0;
        Text[] minValues = new Text[3];
        Text[] maxValues = new Text[3];
        Text tmaxlabel = new Text("TMAX");
        Text tminlabel = new Text("TMIN");
        // iterate through values and aggregate
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
        Text minSumText = new Text(Double.toString(minSum));
        Text maxSumText = new Text(Double.toString(maxSum));
        Text minCountText = new Text(Double.toString(minCount));
        Text maxCountText = new Text(Double.toString(maxCount));
        minValues[0] = tminlabel;
        minValues[1] = minSumText;
        minValues[2] = minCountText;
        maxValues[0] = tmaxlabel;
        maxValues[1] = maxSumText;
        maxValues[2] = maxCountText;
        // emit the min values
        context.write(key, new TextArrayWritable(minValues));
        // emit the max values
        context.write(key, new TextArrayWritable(maxValues));
    }
}