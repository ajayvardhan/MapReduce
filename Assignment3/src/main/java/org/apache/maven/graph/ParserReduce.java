package org.apache.maven.graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
Each parser reducer call receives a list of values for each page name. All the pages names, including the ones in the
master list and the links in each adjacency list is emitted from the mapper. This way, if any of the links in the
adjacency list of the page names does not have any outlinks, those will be emitted with just an empty string as the
value. Hence we can easily identify all the dangling nodes.
 */

public class ParserReduce extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        String output = "";
        String page = key.toString();
        // global counter is updated for each reduce call since the reducer will be called for each unique page
        // only once. this can be used to calculated the total nodes.
        context.getCounter(App.globalCounter.TOTALNODES).increment(1);
        // the adjacency list is reconstructed for this page
        for(Text val:values){
            output+=val.toString();
        }
        // if the node is a dangling node, it will be emitted to the output with an empty adjacency list
        if(output.equals("")){
            context.write(new Text(page),new Text("[]"));
        }
        // else it will be emitted with it's adjacency list.
        else{
            context.write(new Text(page),new Text(output));
        }
    }
}