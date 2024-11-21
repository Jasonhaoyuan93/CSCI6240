package edu.neu.cs6620.hw4;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HComputeDriver  {

    public static class HComputeMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //pull hbase

            if(value==null|| StringUtils.isBlank(value.toString())) return;
            String[] valueParts = value.toString().split("\t");
            String[] entryKeyParts = valueParts[0].split("-");
            String entryYear = entryKeyParts[1];
            // Configure the scan for year 2008
            if(!"2008".equalsIgnoreCase(entryYear)) return;
            String airline = entryKeyParts[0];
            context.write(new Text(airline), value);
        }
    }

    public static class HComputeReducer extends Reducer<Text, Text, Text, Text> {

        private Connection connection;
        private Table table;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = HBaseConfiguration.create();
            if (hBaseSite !=null){
                conf.addResource(new File(hBaseSite).toURI().toURL());
            }
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("FlightData"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] avgDelay = new int[12];
            // Iterate through values for the given airline
            for (Text value : values) {
                String[] parts = value.toString().split("-");
                int month = Integer.parseInt(parts[2].split("\t")[0]); // Convert month to integer (1-12)
                avgDelay[month-1] = processEntry(value);
            }

            // Build the result for the airline
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < 12; i++) {
                result.append("(").append(i + 1).append(",").append(avgDelay[i]).append("), ");
            }

            // Remove the trailing comma and space if present
            if(result.length()>2) result.setLength(result.length() - 2);

            // Emit the airline and its average delays per month
            context.write(key, new Text(result.toString()));
        }

        protected int processEntry(Text entry){

            String[] entryParts = entry.toString().split("\t"); //
            int endIndex = Integer.parseInt(entryParts[1])+1;

            String[] entryKeyParts = entryParts[0].split("-");
            String airline = entryKeyParts[0];
            String entryYear = entryKeyParts[1];
            String entryMonth = entryKeyParts[2];

            Scan scan = new Scan();
            scan.withStartRow((airline+"-"+entryYear+"-"+entryMonth+"-0").getBytes());
            scan.withStopRow((airline+"-"+entryYear+"-"+entryMonth+"-"+endIndex).getBytes());

            // Add only the necessary column to the scan
            scan.addColumn("info".getBytes(), "ArrDelayMinutes".getBytes());
            try(ResultScanner scanner = table.getScanner(scan)){
                double monthlySum = 0;
                int recordCount = 0;
                for (Result result : scanner) {
                    double delay = Double.parseDouble(
                            Optional.ofNullable(result.getValue("info".getBytes(), "ArrDelayMinutes".getBytes()))
                                    .map(Object::toString)
                                    .orElseGet(()->"0.00"));
                    monthlySum+=delay;
                    recordCount++;
                }
                if(recordCount==0) recordCount++;
                return (int) Math.ceil(monthlySum/recordCount);
            }catch (Exception e){
                System.out.println(scan);
                e.printStackTrace();
            }
            return 0;
        }

        @Override
        protected void cleanup(Context context) throws IOException {
            if (table != null) table.close();
            if (connection != null) connection.close();
        }
    }

    public static class HComputePartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.toString().hashCode()%numPartitions;
        }
    }

    private static String hBaseSite;

    public static void main(String[] args) {
        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "H-COMPUTE");

            job.setJarByClass(HComputeDriver.class);
            job.setMapperClass(HComputeMapper.class);
            job.setReducerClass(HComputeReducer.class);
            job.setPartitionerClass(HComputePartitioner.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(10);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if(args.length>=3){
                hBaseSite = args[2];
            }

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(1);
    }
}
