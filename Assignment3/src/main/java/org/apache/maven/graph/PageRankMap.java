package org.apache.maven.graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;

/*
The mapper task for page rank calculation gets the input from the pre-processing stage. This has all the pages, including
the dangling nodes. Mapper reads each line, and creates a node object for each page. The page is emitted with it's node object
to inform the reducer that this is a node. Each link in it's adjacency list is emitted along with the calculated
page rank to the reducer to calculate the new page rank for this page.
 */

public class PageRankMap extends Mapper<Object, Text, Text, WritableComparableObject> {
    private double delta;

    public void setup(Context context) throws IOException, InterruptedException{
        // a delta variable is initialised globally for every map task that will be updated in the end of the tast
        // the current updated delta variable from the previous map tast is retrieved from the config variable
        String s = context.getConfiguration().get("CURRENT_DELTA");
        delta = Double.parseDouble(s);
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // since the numbers were too small, used a decimat formatter to format the numbers to a reasonable decimal points.
        DecimalFormat df = new DecimalFormat("#.################");
        df.setRoundingMode(RoundingMode.CEILING);
        // for the first iteration, the inital page rank value is used for all the nodes
        if(context.getConfiguration().get("iteration").equals("1")){
            String pageRank = context.getConfiguration().get("pageRank");
            String[] page = value.toString().split("\\t");
            page[1] = page[1].trim();
            WritableComparableObject node = new WritableComparableObject(new Text(pageRank),new Text(page[1]));
            if(page[1].length()==2) {
                delta+=Double.parseDouble(node.getpageRank().toString());
            }
            context.write(new Text(page[0]),node);
            if(page[1].length()>2) {
                page[1] = page[1].substring(1, page[1].length() - 1);
                String[] pages = page[1].split(", ");
                double p = Double.parseDouble(df.format(Double.parseDouble(node.getpageRank().toString())))/(double)pages.length;
                for(String s : pages){
                    context.write(new Text(s),new WritableComparableObject(new Text(df.format(p))));
                }
            }
        }
        else{
            String[] page = value.toString().split("\\t");
            String[] links = page[1].split(":");
            WritableComparableObject node = new WritableComparableObject(new Text(links[1]),new Text(links[0]));
            context.write(new Text(page[0]),node);
            if(links[0].length()==2) {
                delta+=Double.parseDouble(node.getpageRank().toString());
            }
            if(links[0].length()>2) {
                links[0] = links[0].substring(1, links[0].length() - 1);
                String[] pages = links[0].split(", ");
                double p = Double.parseDouble(df.format(Double.parseDouble(node.getpageRank().toString())))/(double)pages.length;
                for(String s : pages){
                    context.write(new Text(s),new WritableComparableObject(new Text(df.format(p))));
                }
            }
        }
    }
    public void cleanup(Context context) throws IOException, InterruptedException{
        context.getConfiguration().set("CURRENT_DELTA",Double.toString(delta));
    }
}