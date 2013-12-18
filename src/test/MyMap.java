package test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * This is the main Mapper class. 
 * 
 * @author domenicocitera
 */
public class MyMap extends 
        Mapper<LongWritable, Text, Text, IntWritable> 
{

    /**

     * @param key - Input key - The line offset in the file - ignored.
     * @param value - Input Value - This is the line itself.
     * @param context - Provides access to the OutputCollector and Reporter.
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws 
            IOException, InterruptedException {

        String[] line = value.toString().split(" ");

        String outputKey = line[1];
        System.out.printf("\n"+line[1]+" "); //test

        // The output `value` is the requests
        int outputValue = Integer.parseInt(line[2]); //test
        System.out.printf(line[2]);
        // Record the output in the Context object
        context.write(new Text(outputKey), new IntWritable(outputValue));
    }
}
