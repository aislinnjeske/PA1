package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.util.*;
import java.io.IOException;
import java.util.TreeMap;

public class Profile3NGramReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
    private Map<String, Integer> allUnigramsAndFrequencyInCorpus = new HashMap<String, Integer>();
    
    //key = document ID, value = word
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val : values){  
//             Keeping track of frequency of unigrams for corpus for Profile3
            if(allUnigramsAndFrequencyInCorpus.containsKey(val.toString())){
                int count = allUnigramsAndFrequencyInCorpus.get(val.toString());
                count++;
                allUnigramsAndFrequencyInCorpus.put(val.toString(), count);
            } else {
                allUnigramsAndFrequencyInCorpus.put(val.toString(), 1);
            }
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
//         Sort map in decreasing order by value for profile 3
        LinkedHashMap<String, Integer> sortedUnigrams = new LinkedHashMap<String,Integer>();
        allUnigramsAndFrequencyInCorpus.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedUnigrams.put(x.getKey(), x.getValue()));
        
//         Print unigrams for profile3
        int count = 0;
        for(String entry : sortedUnigrams.keySet()){
        
            if(count < 500){
                context.write(new Text(entry), new IntWritable(sortedUnigrams.get(entry)));
                count++;
            } else {
                break;
            }
        }
    }
}
