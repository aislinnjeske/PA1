package cs435.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.*;

public class P3NGramMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Map<String, Integer> frequencies = new HashMap<String, Integer>();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = splitLine(value);
        
        if(line != null){        
            String[] words = line[2].split("\\s+\\t+\\n+");
            for(String w : words){
                String word = w.toLowerCase().replaceAll("[^a-zA-Z0-9]+","");
                
                if(frequencies.containsKey(word)){
                    int count = frequencies.get(word);
                    count++;
                    frequencies.put(word, count);
                } else {
                    frequencies.put(word, 1);
                }                
            }
        }
    }
    
    private String[] splitLine(Text value){
        if(value.toString() == null || value.toString().length() == 0){
            return null;
        } else {
            //Line format: Title <====> Unique Document ID <====> Contents
            return value.toString().split("<====>");
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LinkedHashMap<String, Integer> sortedFrequencies = new LinkedHashMap<String,Integer>();

        frequencies.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedFrequencies.put(x.getKey(), x.getValue()));
        
        int unigramCount = 0;
        for(String key : frequencies.keySet()){
        
            if(unigramCount == 500){
                break;
            } else {
                context.write(new Text(key), new IntWritable(frequencies.get(key)));
                unigramCount++;
            }
        }
    }

}
