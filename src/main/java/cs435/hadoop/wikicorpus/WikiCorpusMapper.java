package cs435.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;

public class WikiCorpusMapper extends Mapper<Object, Text, IntWritable, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
        if(value.toString() == null || value.toString().length() == 0){
            return;
        }
        
        //Line format: Title <====> Unique Document ID <====> Contents
        String[] line = value.toString().split("<====>", 3);
        int documentID = Integer.parseInt(line[1]);
        
        StringTokenizer itr = new StringTokenizer(line[2]);
        while(itr.hasMoreTokens()){
            String word = itr.nextToken().toLowerCase();
            int index = word.indexOf('\'');
        
            if(index != -1){
                StringBuilder builder = new StringBuilder(word);
                builder.deleteCharAt(index);
                word = builder.toString();
            }
            Text text = new Text();
            text.set(word);
            context.write(new IntWritable(documentID), text);
        }
    }
}
