package org.eduonix;

import com.google.common.collect.Maps;
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
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ubu on 4/21/14.
 */
public class HiveRunner {

    private static final String test_data_input_directory  =  "./testData";
    private static final String test_data_output_directory  =  "./testDataOut";
    public static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final String HIVE_JDBC_EMBEDDED_CONNECTION = "jdbc:hive2://";


    static {
        try{
            Class.forName("org.apache.hadoop.hive.ql.exec.mr.ExecDriver");
            System.out.println("found exec driver !!!!!!!!!!!!!!!!");
        }
        catch(Throwable t) {
            throw new RuntimeException(t);
        }
        try{
            //Class.forName("org.apache.hadoop.hive.ql.exec.mr.ExecDriver");
        }
        catch(Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, SQLException {

        int records = 10;
        /**
         * Setup configuration with prop.
         */
        Configuration conf = new Configuration();
        conf.setInt("records", records);

        Path test_data_in = new Path(test_data_input_directory,"lineitem.txt"); //
        Path test_data_out = new Path(test_data_output_directory);
        Path hive_data_in = new Path(test_data_output_directory, "part-r-00000");

        Path outTablePath = new Path (test_data_out.getParent(),"testDataOut");
        System.out.println("FINAL OUTPUT Path : " + outTablePath);


        Job createInput=  createJob(test_data_in, test_data_out,  conf);
        createInput.waitForCompletion(true);
        JobID jId= createInput.getJobID();


        List<String> lines = Files.readLines(FileSystem.getLocal(conf).pathToFile(hive_data_in), Charset.defaultCharset());
        System.out.println("output : " + FileSystem.getLocal(conf).pathToFile(hive_data_in));
        for(String l : lines){
            System.out.println(l);

        }


        Statement stmt = getConnection();

        // DELETE
        stmt.execute("DROP TABLE IF EXISTS testdata");




        // CREATE
      // sample row
      //  99	1995-01-27,COLLECT COD,AIR, cajole furiously blithely ironic ideas

        final String create = "CREATE TABLE testdata ("
                + "  delivery STRING,"
                + "  payment STRING,"
                + "  secret STRING"
                + ") ROW FORMAT "
                + "DELIMITED FIELDS TERMINATED BY ',' "
                + "LINES TERMINATED BY '\n' ";

        boolean res = stmt.execute(create);
        System.out.println("Execute return code : " +res);



        // INSERT
        String insertData ="LOAD DATA INPATH '/home/ubu/MapReduceHive/testDataOut/part-r-00000' OVERWRITE INTO TABLE `default.testdata` ";

        stmt.execute(insertData);

        // SELECT
        String finalOutput = "SELECT * FROM testdata";
        System.out.println(finalOutput);

        ResultSet resultSet = stmt.executeQuery(finalOutput);

        List<Map> resultList =  resultSetToArrayList(resultSet);

        for(Map m : resultList) {
            System.out.println(m.get("payment"));

        }



    }


    // Utility methods

    public static List<Map> resultSetToArrayList(ResultSet rs)
            throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        ArrayList<Map> list = new ArrayList<Map>(50);
        while (rs.next()) {
            HashMap row = new HashMap(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(md.getColumnName(i), rs.getObject(i));
            }
            list.add(row);
        }

        return list;
    }


    private static  Statement getConnection() throws ClassNotFoundException,
            SQLException {
        Class.forName(HIVE_JDBC_DRIVER);
        Connection con = DriverManager.getConnection(
                HIVE_JDBC_EMBEDDED_CONNECTION, "", "");
        System.out.println("hive con = " + con.getClass().getName());
        Statement stmt = con.createStatement();
        return stmt;
    }


    public static Job createJob(Path input, Path output, Configuration conf) throws IOException {
        Job job=new Job(conf, "MapRedHive_"+System.currentTimeMillis());
        // recursively delete the data set if it exists.
        FileSystem.get(conf).delete(output, true);
        job.setJarByClass(HiveRunner.class);
        job.setMapperClass(PreprocessorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    static class PreprocessorMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text wordKey = new Text();
        private Text wordValue = new Text();
        int counter;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("\\|");

            // 3|4297|1798|1|45|54058.05|0.06|0.00|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco|
            // 12 to 15

            wordKey.set("");
            counter++;
            String clean = tokens[15]; //
            clean = clean.replaceAll("[.]","").replaceAll(",", "").replaceAll("-","").replaceAll("--","");


            wordValue.set(tokens[12]+","+tokens[13]+","+tokens[14]+","+clean);
            context.write(wordKey, wordValue);

           }

        }
    }
