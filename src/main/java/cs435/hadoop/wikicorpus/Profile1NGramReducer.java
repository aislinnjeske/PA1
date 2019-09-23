package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.io.IOException;
import java.util.TreeMap;

public class Profile1NGramReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
    private Set<String> allUnigramsInCorpus = new TreeSet<>();
    
    //key = document ID, value = word
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {        
        for(Text val : values){
            //Adding all unigrams to set for Profile 1
            allUnigramsInCorpus.add(val.toString());
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //Print unigrams for profile1
        Iterator itr = allUnigramsInCorpus.iterator();
        int count = 0;
        
        while(itr.hasNext() && count < 500){
            context.write(null, new Text((String) itr.next()));
            count++;
        }
    }
}
