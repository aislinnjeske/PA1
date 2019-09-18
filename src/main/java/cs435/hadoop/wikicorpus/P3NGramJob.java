package cs435.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class P3NGramJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        //Name of the MR job. You'll see it in the YARN webapp
        Job job = Job.getInstance(conf, "N-Gram of Wiki Corpus Profile 3");
        
        job.setJarByClass(P3NGramJob.class);
        job.setMapperClass(P3NGramMapper.class);
        job.setReducerClass(P3NGramReducer.class);
        
        //Output from reducer 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        
        //Path to input and output HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
