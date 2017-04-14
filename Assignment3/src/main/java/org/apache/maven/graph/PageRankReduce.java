package org.apache.maven.graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
/*
the reducer of a page rank program receives a node object and the list of page rank values calculated in the map phase
this is used to calculate the updated page rank for this node and emitted
 */
public class PageRankReduce extends Reducer<Text,WritableComparableObject,Text,Text> {
    public void reduce(Text key, Iterable<WritableComparableObject> values,
                       Context context) throws IOException, InterruptedException {
        double pageRank = 0.0;
        // alpha value is set as 0.85 based on various reliable sources
        double alpha = 0.85;
        double beta = 0.15;
        // since the numbers were too small, used a decimat formatter to format the numbers to a reasonable decimal points.
        DecimalFormat df = new DecimalFormat("#.################");
        df.setRoundingMode(RoundingMode.CEILING);
        // the delta value from the previous iteration is retrieved from the configuration variable
        String s = context.getConfiguration().get("PREVIOUS_DELTA");
        double delta = Double.parseDouble(s);
        delta = Double.parseDouble(df.format(delta));
        String adjacencyList = "";
        for (WritableComparableObject val: values){
            // if the value is a node, the current page's adjacency list is updated
            if(val.getisNode().toString().equals("true")){
                adjacencyList = val.getadjacencyList().toString();
            }
            // if note, the page rank retrieved is accumulated to the local variable
            else{
                pageRank+=Double.parseDouble(val.getpageRank().toString());
            }
        }
        // from the accumulated page rank, final page rank is calculated
        double totalNodes = Double.parseDouble(context.getConfiguration().get("totalNodes"));
        double p = Double.parseDouble(df.format(alpha/totalNodes)) + Double.parseDouble(df.format(beta*((delta/totalNodes) + pageRank)));
        String output = adjacencyList + ": " + df.format(p);
        // the page is emitted along with it's updated page rank and adjacency list
        context.write(key, new Text(output));
    }
}