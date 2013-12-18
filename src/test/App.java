package test;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The main application class.
 *
 * @author domenicocitera
 */
public class App {

    private static final String root = "/user/hive/warehouse";
    private static final String dbName = "";
    private static final String tableName = "bda_wikidump_dcitera_olopez";
    private static final String outRootPath = "/user/cloudera/";
    private static boolean dev = true;

    public static void main(String[] args) throws Exception {
        try {

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

            //CREA LISTA
            List<Path> listpath = new ArrayList<Path>();
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(root + "/" + dbName + tableName + "/"));
            for (int i = 0; i < status.length; i++) {
                FileStatus[] status2 = fs.listStatus(status[i].getPath());
                for (int j = 0; j < status2.length; j++) {
                    System.out.println(status2[j].getPath());
                    listpath.add(status2[j].getPath());
                }
            }

            // Create the job specification object
            Job job = new Job();
            job.setJarByClass(App.class);
            job.setJobName("Most_Visited_Day");

            // Setup input and output paths
            //for (Path p:listpath) {
            FileInputFormat.addInputPath(job, listpath.get(0));
            Path outFilesPath=new Path(outRootPath+"/mes_vista_"+year+"-"+month+"-"+day);
            fs.delete(outFilesPath, true);
            FileOutputFormat.setOutputPath(job, outFilesPath);

            // Set the Mapper and Reducer classes
            job.setMapperClass(MyMap.class);
            job.setReducerClass(MyRed.class);

            // Specify the type of output keys and values
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // Wait for the job to finish before terminating
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Job ended.");
    }
}
