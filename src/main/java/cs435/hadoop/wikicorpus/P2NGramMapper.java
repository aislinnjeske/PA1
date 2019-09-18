package cs435.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.*;

public class P2NGramMapper extends Mapper<Object, Text, IntWritable, Text> {
    private Set<String> unigrams = new TreeSet<>();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = splitLine(value);
        
        if(line != null){
            int documentID = Integer.parseInt(line[1]);
        
            StringTokenizer itr = new StringTokenizer(line[2]);
            while(itr.hasMoreTokens()){
                String word = itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]+","");
                unigrams.add(word);
            }
        }
    }
    
    private String[] splitLine(Text value){
        if(value.toString() == null || value.toString().length() == 0){
            return null;
        } else {
            //Line format: Title <====> Unique Document ID <====> Contents
            return value.toString().split("<====>", 3);
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //Print top 500 unigrams
        int unigramCount = 0;
        Iterator itr = unigrams.iterator();
        while(itr.hasNext() && unigramCount < 500){
            context.write(new IntWritable(0), new Text((String) itr.next()));
            unigramCount++;
        }

    }

}
