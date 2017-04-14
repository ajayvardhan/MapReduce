package org.apache.maven.graph;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;
/*
TopK reducer receives all the local top 100 pages from each map tasks to a single reduce call since all the pages are
emitted with a null key. The same method of using a local TreeMap to find the top 100 is used in reducer as well.
 */
public class TopKReduce extends Reducer<NullWritable,Text,Text,NullWritable> {
    private TreeMap<String,String> topK;

    // a global TreeMap is initialised for the reduce task
    public void setup(Context context) throws IOException, InterruptedException{
        topK = new TreeMap<>();
    }
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val : values){
            String[] s = val.toString().split(":");
            // for each page, the page along with it's pagerank is inserted into the treemap, which will automatically
            // have the data sorted according to the pagerank
            topK.put(s[1],s[0]);
            // if the sie exveeds 100 values, remove the page with the smallest pagerank
            if(topK.size()>100){
                topK.remove(topK.firstKey());
            }
        }
    }
    public void cleanup(Context context) throws IOException, InterruptedException{
        // emit the top 100 pages in descending order to the final output directory
        for(String key:topK.descendingKeySet()){
            String s = topK.get(key) + " : " + key;
            context.write(new Text(s),NullWritable.get());
        }
    }
}