package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.io.IOException;

public class Profile1Reducer extends Reducer<IntWritable, Text, IntWritable, Text>{
    private Set<String> sortedUnigrams = new TreeSet<>();
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //key = document ID, value = word
    
        //Profile 1: List of unigrams that occurred at least once in corpus, sort in ascending alphabetical order
        for(Text val : values){
            sortedUnigrams.add(val.toString());
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator itr = sortedUnigrams.iterator();
        while(itr.hasNext()){
            context.write(null, new Text((String) itr.next()));
        }
    }
}
