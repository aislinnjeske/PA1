package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.util.*;
import java.io.IOException;
import java.util.TreeMap;

public class NGramWikipediaReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
    private MultipleOutputs mos;
    private Set<String> allUnigramsInCorpus = new TreeSet<>();
    
    //key = document ID, value = word
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
        Map<String, Integer> unigrams = new HashMap<String, Integer>();
        LinkedHashMap<String, Integer> sortedUnigrams = new LinkedHashMap<String,Integer>();
    
        
        for(Text val : values){
            //Adding all unigrams to set for Profile 1
            allUnigramsInCorpus.add(val.toString());
        
            //Keeping track of frequency of unigrams per documentID for Profile2
            if(unigrams.containsKey(val.toString())){
                int count = unigrams.get(val.toString());
                count++;
                unigrams.put(val.toString(), count);
            } else {
                unigrams.put(val.toString(), 1);
            }
        }
        
        //Sort in decreasing order by value
        unigrams.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedUnigrams.put(x.getKey(), x.getValue()));
        
        for(String entry : sortedUnigrams.keySet()){
            mos.write(new IntWritable(sortedUnigrams.get(entry)), new Text(entry), "DocumentID=" + key.toString());
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator itr = allUnigramsInCorpus.iterator();
        while(itr.hasNext()){
            mos.write("Profile1", null, new Text((String) itr.next()));
        }
        mos.close();
    }
}
