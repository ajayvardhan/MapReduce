package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
Mapper class iterates through the input values and emits a WritableComparableObject (station ID, year)
and a TextArrayWritable (year, Temperature label, temperature, count). For each record in the input,
the map emits the output with a count of 1 if the record is either TMAX or TMIN.
 */

public class SecondarySortMapper extends Mapper<Object, Text, WritableComparableObject, TextArrayWritable> {
    private Text[] values = new Text[4];
    private Text stationID = new Text();
    private Text year = new Text();
    private Text label = new Text();
    private Text temperature = new Text();
    private Text count = new Text();
    @Override
    protected void map(Object key, Text value,
                       Context context) throws IOException, InterruptedException {
        String[] temp = new String[10];
        temp = value.toString().split(",");
        if ((temp[2].equals("TMAX") || temp[2].equals("TMIN") && !temp[3].equals("") && !temp[0].equals(""))){
            stationID.set(temp[0]);
            year.set(temp[1].substring(0,4));
            label.set(temp[2]);
            temperature.set(temp[3]);
            count.set("1");
            values[0] = year;
            values[1] = label;
            values[2] = temperature;
            values[3] = count;
            context.write(new WritableComparableObject(stationID, year), new TextArrayWritable(values));
        }
    }
}