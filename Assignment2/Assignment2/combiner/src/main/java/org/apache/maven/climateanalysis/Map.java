package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
Mapper class iterates through the input values and emits the station ID and a TextArrayWritable with
Temperature label, temperature and count. For each record in the input, the map emits the output with
a count of 1 if the record is either TMAX or TMIN.
 */
public class Map extends Mapper<Object, Text, Text, TextArrayWritable> {
    private Text[] values = new Text[3];
    private Text stationID = new Text();
    private Text label = new Text();
    private Text temperature = new Text();
    private Text count = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] temp = new String[10];
        temp = value.toString().split(",");
        if ((temp[2].equals("TMAX") || temp[2].equals("TMIN") && !temp[3].equals(""))){
            stationID.set(temp[0]);
            label.set(temp[2]);
            temperature.set(temp[3]);
            count.set("1");
            values[0] = label;
            values[1] = temperature;
            values[2] = count;
            // Custom writable object TextArrayWritable which is an array of Texts, which are writable themselves
            context.write(stationID, new TextArrayWritable(values));
        }
    }
}