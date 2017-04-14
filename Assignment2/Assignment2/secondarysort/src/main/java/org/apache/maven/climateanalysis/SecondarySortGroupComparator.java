package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

/*
Gets the composite key from the mapper output, compares just the station ID part of the key and returns the result.
Since the key contains 2 values, the grouping comparator helps the system to send all records with the same
station ID to a sincle reduce task.
 */

public class SecondarySortGroupComparator extends WritableComparator {
    protected SecondarySortGroupComparator() {
        super(WritableComparableObject.class, true);
    }
    public int compare(WritableComparable w1, WritableComparable w2) {
        WritableComparableObject wc1 = (WritableComparableObject) w1;
        WritableComparableObject wc2 = (WritableComparableObject) w2;
        return wc1.getStationID().compareTo(wc2.getStationID());
    }
}