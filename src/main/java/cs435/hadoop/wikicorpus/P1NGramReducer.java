package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.util.*;
import java.io.IOException;
import java.util.TreeMap;

public class P1NGramReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
//     private MultipleOutputs mos;
    private Set<String> allUnigramsInCorpus = new TreeSet<>();
//     private Map<String, Integer> allUnigramsAndFrequencyInCorpus = new HashMap<String, Integer>();
    
    //key = document ID, value = word
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//         mos = new MultipleOutputs(context);
//         Map<String, Integer> unigrams = new HashMap<String, Integer>();
//         LinkedHashMap<String, Integer> sortedUnigrams = new LinkedHashMap<String,Integer>();
    
        
        for(Text val : values){
            //Adding all unigrams to set for Profile 1
            allUnigramsInCorpus.add(val.toString());
        
//             Keeping track of frequency of unigrams per documentID for Profile2
//             if(unigrams.containsKey(val.toString())){
//                 int count = unigrams.get(val.toString());
//                 count++;
//                 unigrams.put(val.toString(), count);
//             } else {
//                 unigrams.put(val.toString(), 1);
//             }
            
//             Keeping track of frequency of unigrams for corpus for Profile3
//             if(allUnigramsAndFrequencyInCorpus.containsKey(val.toString())){
//                 int count = allUnigramsAndFrequencyInCorpus.get(val.toString());
//                 count++;
//                 allUnigramsAndFrequencyInCorpus.put(val.toString(), count);
//             } else {
//                 allUnigramsAndFrequencyInCorpus.put(val.toString(), 1);
//             }
        }
        
//         Sort in decreasing order by value for profile2
//         unigrams.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedUnigrams.put(x.getKey(), x.getValue()));
        
//         Print unigrams for each documentID in a new file for profile2
//         context.write(key, new Text(": "));
//         for(String entry : sortedUnigrams.keySet()){
//             context.write(new IntWritable(sortedUnigrams.get(entry)), new Text(entry));
//         }
//         context.write(null, new Text("\n"));
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //Print unigrams for profile1
        int unigramCount = 0;
        Iterator itr = allUnigramsInCorpus.iterator();
        while(itr.hasNext() && unigramCount < 500){
            context.write(null, new Text((String) itr.next()));
            unigramCount++;
        }
/*        
        Sort map in decreasing order by value for profile 3
        LinkedHashMap<String, Integer> sortedUnigrams = new LinkedHashMap<String,Integer>();
        allUnigramsAndFrequencyInCorpus.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedUnigrams.put(x.getKey(), x.getValue()));
        
        Print unigrams for profile3
        int profileThreeCount = 0;
        Iterator iterator = sortedUnigrams.keySet().iterator();
        while(iterator.hasNext() && profileThreeCount < 500){
            String key = (String) iterator.next();
            mos.write("Profile3", new IntWritable(sortedUnigrams.get(key)), new Text(key));
            profileThreeCount++;
        }

        mos.close();*/
    }
    
}
