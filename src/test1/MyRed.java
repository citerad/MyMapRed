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
public class MyRed extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * @param key - Input key - Name of the context
     * @param values - Input Value - Iterator request context
     * @param context - Used for collecting output
     * @throws IOException
     * @throws InterruptedException
     */
    private final Text maxWord = new Text();
    private int max = 0;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum = sum + value.get();

            if (sum >= max) {
                max = sum;
                maxWord.set(key);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(maxWord, new IntWritable(max));

    }
}
