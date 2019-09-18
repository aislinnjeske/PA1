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

public class P2NGramJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        //Name of the MR job. You'll see it in the YARN webapp
        Job job = Job.getInstance(conf, "N-Gram of Wiki Corpus Profile 2");
        
        job.setJarByClass(P2NGramJob.class);
        job.setMapperClass(P2NGramMapper.class);
        job.setReducerClass(P2NGramReducer.class);
        
        //Output from reducer 
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        
        //Path to input and output HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
