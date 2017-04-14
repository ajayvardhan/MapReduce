package org.apache.maven.graph;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/*
The TopK reducer recieves the final page rank output file after 10 iterations. The mapper finds the local top 100 for
each input chunk it receives using a TreeMap data structure.
 */
public class TopKMap extends Mapper<Object, Text, NullWritable, Text>  {
    private TreeMap<String,String> topK;

    // a global TreeMap is initialised that is to be used by each map task
    public void setup(Context context) throws IOException, InterruptedException{
        topK = new TreeMap<>();
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] page = value.toString().split("\\t");
        String[] links = page[1].split(":");
        // for each page, the page along with it's pagerank is inserted into the treemap, which will automatically
        // have the data sorted according to the pagerank
        topK.put(links[1],page[0]);
        // if the sie exveeds 100 values, remove the page with the smallest pagerank
        if(topK.size()>100){
            topK.remove(topK.firstKey());
        }

    }
    public void cleanup(Context context) throws IOException, InterruptedException{
        // emit the top 100 pages in descending order to the a single reducer using a null key
        for(String key:topK.descendingKeySet()){
            String s = topK.get(key) + ":" + key;
            context.write(NullWritable.get(), new Text(s));
        }
    }
}