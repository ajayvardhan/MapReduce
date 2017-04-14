package org.apache.maven.graph;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.math.RoundingMode;
import java.text.DecimalFormat;

/*
Driver class where the configuration is defined and all the jobs are created and run. The mapreduce jobs are chained
in such a way that the output for the previous job is an input for the next job.
 */

public class App
{
    // The global counter which is used to distribute data amongst different jobs.
    public enum globalCounter{
        TOTALNODES, // updated by the parsing job
        CURRENT_DELTA; // updated by the page rank jobs in each iteration to pass on the delta to the next iteration
    }
    public static void main( String[] args ) throws Exception {
        // since the numbers were too small, used a decimat formatter to format the numbers to a reasonable decimal points.
        DecimalFormat df = new DecimalFormat("#.################");
        df.setRoundingMode(RoundingMode.CEILING);

        // pre-processing parsing job
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: PageRank <in> [<in>...] <out>");
            System.exit(2);
        }
        Job parsing = Job.getInstance(conf, "Parsing");
        parsing.setJarByClass(App.class);
        parsing.setMapperClass(ParserMap.class);
        parsing.setReducerClass(ParserReduce.class);
        parsing.setMapOutputKeyClass(Text.class);
        parsing.setMapOutputValueClass(Text.class);
        parsing.setMapOutputValueClass(Text.class);
        parsing.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(parsing, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(parsing, new Path(otherArgs[1]));
        parsing.waitForCompletion(true);

        // the total number of nodes that the parsing job calculates is retrieved from the global counter
        double pageRank = (double) parsing.getCounters().findCounter(globalCounter.TOTALNODES).getValue();

        // a configuration variable is created for the total nodes and initial page rank for the page rank jobs to access
        conf.set("totalNodes",Double.toString(pageRank));
        conf.set("pageRank",df.format(1.0/pageRank));
        String finalInputPath = "";

        // configuration variables are initialised to help with the delta calculations during the page rank jobs iterations
        conf.set("PREVIOUS_DELTA",Double.toString(0.0));
        conf.set("CURRENT_DELTA",Double.toString(0.0));

        // page rank job iterations
        for(int i=1;i<=10;i++){
            // creating separate output paths for each iteration based on the iteration counter
            String path = otherArgs[1]+"/PageRankOutput/Output"+Integer.toString(i);
            String inputPath = otherArgs[1];
            if(i==10){
                // for the final iteration, the output is put in a final output path and this path is then passed on
                // to the next TopK job as input path
                path = otherArgs[1]+"/PageRankOutput/FinalOutput";
                finalInputPath = path;
            }
            if (i > 1) {
                // changing the input paths for each iteration as the output path of the previous iteration
                inputPath = otherArgs[1]+"/PageRankOutput/Output"+Integer.toString(i-1);
            }
            // an iteration variable is created for the jobs to find out which iteration is currently happening
            // to perform calculations based on that
            conf.set("iteration",Integer.toString(i));
            Job pagerank = Job.getInstance(conf, "Page Rank");
            pagerank.setJarByClass(App.class);
            pagerank.setMapperClass(PageRankMap.class);
            pagerank.setReducerClass(PageRankReduce.class);
            pagerank.setMapOutputKeyClass(Text.class);
            pagerank.setMapOutputValueClass(WritableComparableObject.class);
            pagerank.setOutputKeyClass(Text.class);
            pagerank.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(pagerank, new Path(inputPath));
            FileOutputFormat.setOutputPath(pagerank, new Path(path));
            pagerank.waitForCompletion(true);
            // the currently completed job will update the delta variable that will be used by the next iteration.
            // That delta value is being retrieved and sent to the next iteration via the configuration variable
//            long currentDelta = pagerank.getCounters().findCounter(globalCounter.CURRENT_DELTA).getValue();
            double d1 = Double.parseDouble(conf.get("CURRENT_DELTA"));
//            double d1 = currentDelta/Math.pow(10,10);
            conf.set("PREVIOUS_DELTA",Double.toString(d1));
            conf.set("CURRENT_DELTA",Double.toString(0.0));
        }
        // the TopK job input is the output directory of the last iteration of page rank job
        String finalOutputpath = otherArgs[1]+"/TopKOutput";
        Job topK = Job.getInstance(conf, "Top K Pages");
        topK.setJarByClass(App.class);
        topK.setMapperClass(TopKMap.class);
        topK.setReducerClass(TopKReduce.class);
        topK.setMapOutputKeyClass(NullWritable.class);
        topK.setMapOutputValueClass(Text.class);
        topK.setOutputKeyClass(Text.class);
        topK.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(topK, new Path(finalInputPath));
        FileOutputFormat.setOutputPath(topK, new Path(finalOutputpath));
        System.exit(topK.waitForCompletion(true) ? 0 : 1);
    }
}