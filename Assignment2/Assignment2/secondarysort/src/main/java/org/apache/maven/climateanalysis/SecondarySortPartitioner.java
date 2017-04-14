package org.apache.maven.climateanalysis;

import org.apache.hadoop.mapreduce.Partitioner;

/*
Partioner partitions the input data such that all the records that belongs to a single station ID falls
into the same bucket. This way a single station ID goes to a single reducer. The getPartition function
returns gets the mapper output and returns a hashcode to uniquely identify the station ID in the key.
 */

public class SecondarySortPartitioner extends Partitioner<WritableComparableObject, TextArrayWritable> {
    public int getPartition(WritableComparableObject key, TextArrayWritable value, int noOfPartitions){
        System.out.println((Math.abs(key.getStationID().hashCode()) % noOfPartitions));
        return (Math.abs(key.getStationID().hashCode()) % noOfPartitions);
    }
}