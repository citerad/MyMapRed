package test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is the main Mapper class.
 *
 * @author domenicocitera
 */
public class MyMap extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    Text outKey = new Text();
    IntWritable outValue = new IntWritable();
    private static Pattern inputPattern = Pattern.compile("(.+)(\\s)(.+)(\\s)(\\d+)(\\s)(\\d+)");

    /**
     *
     * @param key - Input key - The line offset in the file - ignored.
     * @param value - Input Value - This is the line itself.
     * @param context - Provides access to the OutputCollector and Reporter.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        Matcher inputMatch = inputPattern.matcher(value.toString());
//        String[] line = value.toString().split(" ");
//        outKey.set(line[1]);
//        outValue.set(Integer.parseInt(line[2]));
//        context.write(outKey, outValue);
        if (inputMatch.matches()) {
 
            String outKey = inputMatch.group(3);
            int outValue = Integer.parseInt(inputMatch.group(5));

            context.write(new Text(outKey), new IntWritable(outValue));
        }
    }
}
