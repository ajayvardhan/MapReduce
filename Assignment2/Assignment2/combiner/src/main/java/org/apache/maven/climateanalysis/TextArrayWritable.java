package org.apache.maven.climateanalysis;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/*
Custom writable object. Array of Text values that implements the writable interface to
be used as output value for the mapper.
 */

public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }
    public TextArrayWritable(Text[] values) {
        super(Text.class, values);
    }
}