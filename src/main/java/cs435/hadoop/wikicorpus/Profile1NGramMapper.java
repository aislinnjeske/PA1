package cs435.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;

public class Profile1NGramMapper extends Mapper<Object, Text, IntWritable, Text> {
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(value.toString() == null || value.toString().length() == 0){
            return;
        }
        
        //Line format: Title <====> Unique Document ID <====> Contents
        String[] line = value.toString().split("<====>", 3);
        int documentID = Integer.parseInt(line[1]);
        
        StringTokenizer itr = new StringTokenizer(line[2]);
        while(itr.hasMoreTokens()){
            String word = itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]+","");
            
            if(word.length() > 0){
                context.write(new IntWritable(documentID), new Text(word));
            }
        }
    }
}
