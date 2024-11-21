package edu.neu.cs6620.hw4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SecondarySortDriver {

  public static class SecondarySortMapper extends
      Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        String[] fields = value.toString().split(",(?=\\S)");
        int year = Integer.parseInt(clean(fields[0])); // Year
        if(year!=2008) return;
        String airline = clean(fields[8]); // Carrier
        int month = Integer.parseInt(clean(fields[2])); // Month
        String delayStr = clean(fields[37]); // ArrDelayMinutes
        double delay = delayStr.isEmpty()?0D:Double.parseDouble(delayStr);

        context.write(new Text(airline), new Text(month + "," + delay));

      }catch (Exception e){
        e.printStackTrace();
      }
    }
  }

  public static class SecondarySortReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      double[] monthlySum = new double[12]; // Sum of delays for each month
      int[] monthlyCount = new int[12]; // Count of delays for each month

      for (Text value : values) {
        String[] parts = value.toString().split(",");
        int month = Integer.parseInt(parts[0]); // Month
        double delay = Double.parseDouble(parts[1]); // Delay

        monthlySum[month - 1] += delay;
        monthlyCount[month - 1]++;
      }

      // Build the result string for this airline
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < 12; i++) {
            if(monthlyCount[i]==0) monthlyCount[i]++;
            int average = (int) Math.ceil(monthlySum[i] / monthlyCount[i]);
            result.append("(").append(i + 1).append(",").append(average).append("), ");
      }

      // Remove the trailing ", "
      result.setLength(result.length() - 2);
      context.write(key, new Text(result.toString()));
    }
  }

  public static class AirlinePartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
      return key.toString().hashCode()%numPartitions;
    }
  }

  private static String clean(String field){
    return field.trim().replaceAll("\"", "");
  }

  // Driver class
  public static void main(String[] args) throws Exception {
    try{
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Secondary Sort");

      job.setJarByClass(SecondarySortDriver.class);
      job.setMapperClass(SecondarySortMapper.class);
      job.setReducerClass(SecondarySortReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setNumReduceTasks(10);
      // Add the custom partitioner
      job.setPartitionerClass(AirlinePartitioner.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }catch (Exception e){
      e.printStackTrace();
    }
  }
}
