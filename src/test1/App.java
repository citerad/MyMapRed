package test1;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main application class.
 *
 * @author domenicocitera
 */
public class App extends Configured implements Tool {

    private static final String root = "/user/hive/warehouse";
    private static final String dbName = "";
    private static final String tableName = "bda_wikidump_dcitera_olopez";
    private static final String outRootPath = "/user/cloudera/";
    private static final boolean dev = false;

    public static void main(String[] args) throws Exception {

        App driver = new App();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        String year = null, month = null, day = null;

        if (dev) {
            year = "2013";
            month = "11";
            day = "05";
        } else {
            if (args.length == 3) {
                year = args[0];
                month = args[1];
                day = args[2];
            } else {
                throw new Exception("Bad arguments");
            }
        }

        // Create the job specification object
        Job job = new Job(getConf());
        job.setJarByClass(App.class);
        job.setJobName(this.getClass().getName());

        // Setup input and output paths
        String p = resPath(year + month + day);
        FileInputFormat.setInputPaths(job, p);

        Path outFilesPath = new Path(outRootPath + "/mes_vista_" + year + "-" + month + "-" + day);

        // Delete and create if exist
        FileSystem.get(new Configuration()).delete(outFilesPath, true);
        FileOutputFormat.setOutputPath(job, outFilesPath);

        // Set the Mapper and Reducer classes
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyRed.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static String resPath(String ymd) throws IOException {

        Pattern inputPattern = Pattern.compile("(.*)ds=" + ymd + "-(\\d{4})$");
        String pts = "";
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(root + "/" + dbName + tableName + "/"));
        
        for (FileStatus statu : status) {
            Matcher inputMatch = inputPattern.matcher(statu.getPath().toString());
            
            if (inputMatch.matches()) {
                FileStatus[] status2 = fs.listStatus(statu.getPath());
                
                for (FileStatus status21 : status2) {
                    System.out.println(status21.getPath()); //test file input
                    pts = pts.concat(status21.getPath().toString() + ",");
                }
            }
        }
        return pts.substring(0, (pts.length() - 1));
    }
}
