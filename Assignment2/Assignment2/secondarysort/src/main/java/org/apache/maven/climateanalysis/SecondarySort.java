package org.apache.maven.climateanalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
Secondary sort main class where the config and job is initiated.
Mapper, reducer, partitioner, key comparator and group comparator classes are set.
Input and output classes and paths are defined.
 */

public class SecondarySort
{
    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        System.out.println("inside in secondary sort");
        Job job = Job.getInstance(conf, "Climate Analysis");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setSortComparatorClass(SecondarySortComparator.class);
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
        job.setMapOutputKeyClass(WritableComparableObject.class);
        job.setMapOutputValueClass(TextArrayWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}