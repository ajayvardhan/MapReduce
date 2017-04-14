package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

/*
The key comparator for secondary sort compares the composite key that the mapper emits. Mapper uses
WritableComparableObject as the output key. Key comparator gets two of these objects and returns the
result of comparing these two objects.
 */

public class SecondarySortComparator extends WritableComparator {
    public SecondarySortComparator(){
        super(WritableComparableObject.class, true);
    }
    public int compare(WritableComparable w1, WritableComparable w2) {
        WritableComparableObject wc1 = (WritableComparableObject) w1;
        WritableComparableObject wc2 = (WritableComparableObject) w2;
        return wc1.compareTo(wc2);
    }
}