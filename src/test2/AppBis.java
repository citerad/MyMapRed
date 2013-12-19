/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test2;

/**
 *
 * @author domenicocitera
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AppBis {

    private static final String root = "/user/hive/warehouse";
    private static final String dbName = "";
    private static final String tableName = "bda_wikidump_dcitera_olopez";
    private static final String outRootPath = "/user/cloudera/";
    private static boolean dev = false;

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();
        private static Pattern inputPattern = Pattern.compile("(.+)(\\s)(.+)(\\s)(\\d+)(\\s)(\\d+)");

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            Matcher inputMatch = inputPattern.matcher(value.toString());

            if (inputMatch.matches()) {

                word.set(inputMatch.group(3));
                int outValue = Integer.parseInt(inputMatch.group(5));

                output.collect(word, new IntWritable(outValue));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;

            while (values.hasNext()) {
                sum = sum + values.next().get();
            }
            if (sum > 0) {
                output.collect(key, new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String year = null, month = null, day = null;

        if (dev) {
            year = "2013";
            month = "11";
            day = "01";
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
        JobConf job = new JobConf(AppBis.class);
        //job.setJarByClass(MyMapRedB.class);
        job.setJobName("MyJob");

        // Set the Mapper and Reducer classes
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Setup input and output paths
        String p = resPath(year + month + day);
        FileInputFormat.setInputPaths(job, p);

        Path outFilesPath = new Path(outRootPath + "/mes_vista_" + year + "-" + month + "-" + day);

        // Delete and create if exist
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(outFilesPath, true);
        FileOutputFormat.setOutputPath(job, outFilesPath);
        JobClient.runJob(job);
    }

    public static String resPath(String ymd) throws IOException {
        //CREA LISTA
        Pattern inputPattern = Pattern.compile("(.*)ds=" + ymd + "-(\\d{4})$");
        List<Path> listpath = new ArrayList<Path>();
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(root + "/" + dbName + tableName + "/"));
        for (int i = 0; i < status.length; i++) {
            Matcher inputMatch = inputPattern.matcher(status[i].getPath().toString());
            if (inputMatch.matches()) {
                FileStatus[] status2 = fs.listStatus(status[i].getPath());
                for (int j = 0; j < status2.length; j++) {
                    System.out.println(status2[j].getPath());
                    listpath.add(status2[j].getPath());
                }
            }
        }
        String pts = "";
        for (Path p : listpath) {
            pts = pts.concat(p.toString() + ",");
        }
        return pts.substring(0, (pts.length() - 1));
    }
}
