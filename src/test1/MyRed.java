package test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;

/**
 * This is the main Reducer class.
 *
 * @author domenicocitera
 */
public class MyRed extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * @param key - Input key - Name of the context
     * @param values - Input Value - Iterator request context
     * @param context - Used for collecting output
     * @throws IOException
     * @throws InterruptedException
     */
    private int max = 0;
    private Map<String, Integer> maxcont = new HashMap<String, Integer>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum = sum + value.get();

            if (sum >= max) {
                max = sum;
                maxcont.put(key.toString(), sum);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String key : maxcont.keySet()) {
            if (maxcont.get(key) == max) {
                context.write(new Text(key), new IntWritable(max));
            }
        }

    }
}
