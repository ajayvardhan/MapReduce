package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
A single reduce call will recieve all the values for a single station ID. Since the key contains both the station ID
and the year, secondary sort sorts the records by the year. Since the list is sorted, the man is calculated without using
an extra data structure. While iterating the values, the data is aggregated for a single and if the year changes, the
data for the previous year is written to the output and data for the next year starts getting aggregated.
After all the calculation, the reduce call emits (Station ID, [(year1, meanMin1, meanMax1), (year2, meanMin2, meanMax2)... ])
 */

public class SecondarySortReducer extends Reducer<WritableComparableObject, TextArrayWritable, NullWritable, Text> {
    @Override
    protected void reduce(WritableComparableObject key, Iterable<TextArrayWritable> values,
                          Context context) throws IOException, InterruptedException {
        double minSum = 0.0;
        double minCount = 0.0;
        double maxSum = 0.0;
        double maxCount = 0.0;
        // store the first year in the list since the list is sorted by year
        String year = key.getYear().toString();
        // output string where the result will be accumulatead
        String output = "[";
        for (TextArrayWritable ta : values){
            String[] str = ta.toStrings();
            double sum = Double.parseDouble(str[2]);
            double count = Double.parseDouble(str[3]);
            if (year.equals(str[0])){
                if (str[1].equals("TMIN")){
                    minSum+=sum;
                    minCount+=count;
                }
                if (str[1].equals("TMAX")){
                    maxSum+=sum;
                    maxCount+=count;
                }
            }
            // when the values for a particular year gets over, the next year values are processed.
            // For the first accurance of the next year, this else block will get executed
            else{
                Double meanMin = 0.0;
                Double meanMax = 0.0;
                if (minCount!=0.0){
                    meanMin = minSum/minCount;
                }
                if (maxCount!=0.0){
                    meanMax = maxSum/maxCount;
                }
                //mean for the previous year is calculated with the aggregated values and stored to the output text
                output += ("(" + year + ", " + Double.toString(meanMin) + ", " + Double.toString(meanMax) + "), ");
                // year value is updated with the new year
                year = str[0];
                minSum = minCount = maxSum = maxCount = 0.0;
                if (str[1].equals("TMIN")){
                    minSum+=sum;
                    minCount+=count;
                }
                if (str[1].equals("TMAX")){
                    maxSum+=sum;
                    maxCount+=count;
                }
            }
        }
        Double meanMin = 0.0;
        Double meanMax = 0.0;
        if (minCount!=0.0){
            meanMin = minSum/minCount;
        }
        if (maxCount!=0.0){
            meanMax = maxSum/maxCount;
        }
        // after all the values have been processed, the mean for the final year is added to the output
        output += ("(" + year + ", " + Double.toString(meanMin) + ", " + Double.toString(meanMax) + "), ");
        output = output.substring(0, output.length()-2);
        output += "]";
        output = key.getStationID().toString() + ", " + output;
        // list of means for all the years are emitted for this station ID
        context.write(NullWritable.get(), new Text(output));
    }
}