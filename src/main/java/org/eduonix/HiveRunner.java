package org.eduonix;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Statement;
import java.util.List;

/**
 * Created by ubu on 4/21/14.
 */
public class HiveRunner {

    private static final String test_data_input_directory  =  "./testData";
    private static final String test_data_output_directory  =  "./testDataOut";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        int records = 10;
        /**
         * Setup configuration with prop.
         */
        Configuration conf = new Configuration();
        conf.setInt("records", records);

        Path test_data_in = new Path(test_data_input_directory,"lineitem.txt");
        Path test_data_out = new Path(test_data_output_directory);
        Job createInput=  createJob(test_data_in, test_data_out,  conf);
        createInput.waitForCompletion(true);
        JobID jId= createInput.getJobID();

        Path outputfile = new Path(test_data_output_directory,"part-r-00000");
        List<String> lines = Files.readLines(FileSystem.getLocal(conf).pathToFile(outputfile), Charset.defaultCharset());
        System.out.println("output : " + FileSystem.getLocal(conf).pathToFile(outputfile));
        for(String l : lines){
            System.out.println(l);

        }

    }




    public static Job createJob(Path input, Path output, Configuration conf) throws IOException {
        Job job=new Job(conf, "MapRedHive_"+System.currentTimeMillis());
        // recursively delete the data set if it exists.
        FileSystem.get(conf).delete(output, true);
        job.setJarByClass(HiveRunner.class);
        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class MyMapper extends Mapper<Text, Text, LongWritable, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            super.setup(context);
        }

        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException{
            context.write(key, value);
            //TODO: Add multiple outputs here which writes mock addresses for generated users
            //to a corresponding data file.
        };
    }
}
