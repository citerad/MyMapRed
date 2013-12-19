package test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is the main Mapper class.
 *
 * @author domenicocitera
 */
public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text word = new Text();
    private static final Pattern inputPattern = Pattern.compile("(.+)(\\s)(.+)(\\s)(\\d+)(\\s)(\\d+)");

    /**
     * @param key - Input key - The line offset in the file - ignored.
     * @param value - Input Value - This is the line itself.
     * @param context - Provides access to the OutputCollector and Reporter.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Matcher inputMatch = inputPattern.matcher(value.toString());

        if (inputMatch.matches()) {

            word.set(inputMatch.group(3));
            int outValue = Integer.parseInt(inputMatch.group(5));

            context.write(word, new IntWritable(outValue));
        }
    }
}
