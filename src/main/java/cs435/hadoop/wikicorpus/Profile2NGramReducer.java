package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.io.IOException;
import java.util.TreeMap;

public class Profile2NGramReducer extends Reducer<IntWritable, Text, Text, Text>{
    private Map<String, Integer> unigrams = new HashMap<String, Integer>();
    private LinkedHashMap<String, Integer> sortedUnigrams = new LinkedHashMap<String,Integer>();
    private int count = 0;
    
    //key = document ID, value = word
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text val : values){
//             Keeping track of frequency of unigrams per documentID for Profile2
            if(unigrams.containsKey(val.toString())){
                int count = unigrams.get(val.toString());
                count++;
                unigrams.put(val.toString(), count);
            } else {
                unigrams.put(val.toString(), 1);
            }
        }
        
//         Sort in decreasing order by value for profile2
        unigrams.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedUnigrams.put(x.getKey(), x.getValue()));
//         
//         Print unigrams for each documentID in a new file for profile2
        for(String entry : sortedUnigrams.keySet()){
            if(count < 500){
                context.write(new Text(key + "\t" + sortedUnigrams.get(entry)), new Text(entry));
                count++;
            }
        }
        
        //Clear data structures when done
        unigrams.clear();
        sortedUnigrams.clear();
    }
}
