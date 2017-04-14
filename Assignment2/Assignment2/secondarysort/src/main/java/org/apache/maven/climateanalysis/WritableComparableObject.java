package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.*;
import java.io.*;

/*
Custom writable comparable object to be used as a mapper key for secondary sort.
This object has the station ID and year Text values and implements all the writablecomparable
interface methods.
 */

public class WritableComparableObject implements WritableComparable<WritableComparableObject> {
    private Text stationID, year;
    public WritableComparableObject(){
        this.stationID = new Text();
        this.year = new Text();
    }
    public WritableComparableObject(Text stationID, Text year){
        this.stationID = stationID;
        this.year = year;
    }
    public void write(DataOutput out) throws IOException {
        this.stationID.write(out);
        this.year.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.stationID.readFields(in);
        this.year.readFields(in);
    }

    public int compareTo(WritableComparableObject o) {
        int temp = this.stationID.compareTo(o.stationID);
        if (temp == 0){
            return this.year.compareTo(o.year);
        }
        return temp;
    }

    public int hashCode() {
        System.out.println((((Math.abs(this.stationID.hashCode() + this.year.hashCode()))*29)+967));
        return (((Math.abs(this.stationID.hashCode() + this.year.hashCode()))*29)+967);
    }

    public Text getStationID(){
        return this.stationID;
    }
    public Text getYear(){
        return this.year;
    }
}