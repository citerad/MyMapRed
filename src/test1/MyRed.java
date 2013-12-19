package test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;

/**
 * This is the main Reducer class. 
 * 
 * @author domenicocitera
 */

public class MyRed extends 
        Reducer<Text, IntWritable, Text, IntWritable> 
{

    /**
     * @param key - Input key - Name of the context
     * @param values - Input Value - Iterator request context
     * @param context - Used for collecting output
     * @throws IOException
     * @throws InterruptedException 
     */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, 
            Context context) throws IOException, InterruptedException {
        
        // Standard algorithm for finding the max value
        int maxReq = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxReq = Math.max(maxReq, value.get());
        }
        
        context.write(key, new IntWritable(maxReq));
    }
}
