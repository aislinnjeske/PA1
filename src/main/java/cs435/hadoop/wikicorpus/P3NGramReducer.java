package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.util.*;
import java.io.IOException;
import java.util.TreeMap;

public class P3NGramReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
    private Map<String, Integer> allUnigramsAndFrequencyInCorpus = new HashMap<String, Integer>();
    
    //key = word, value = frequency
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int totalFrequncyForWord = 0;
        for(IntWritable val : values){
            totalFrequncyForWord += val.get();
        }
        
        allUnigramsAndFrequencyInCorpus.put(key.toString(), totalFrequncyForWord);

    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    
        LinkedHashMap<String, Integer> sortedByFrequency = new LinkedHashMap<String,Integer>();
        allUnigramsAndFrequencyInCorpus.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedByFrequency.put(x.getKey(), x.getValue()));
        
        int unigramCount = 0;
        for(String key : sortedByFrequency.keySet()){
            if(unigramCount == 500){
                break;
            } else {
                context.write(new Text(key), new IntWritable(sortedByFrequency.get(key)));
                unigramCount++;
            }
        }

    }
    
}
