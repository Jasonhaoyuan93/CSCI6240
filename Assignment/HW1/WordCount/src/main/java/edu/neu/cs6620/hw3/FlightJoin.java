package edu.neu.cs6620.hw3;

import static edu.neu.cs6620.hw3.Constants.ARR_DELAY_MINUTES;
import static edu.neu.cs6620.hw3.Constants.ARR_TIME;
import static edu.neu.cs6620.hw3.Constants.DEP_TIME;
import static edu.neu.cs6620.hw3.Constants.DEST;
import static edu.neu.cs6620.hw3.Constants.FLIGHT_DATE;
import static edu.neu.cs6620.hw3.Constants.JFK;
import static edu.neu.cs6620.hw3.Constants.MONTH;
import static edu.neu.cs6620.hw3.Constants.ORD;
import static edu.neu.cs6620.hw3.Constants.ORIGIN;
import static edu.neu.cs6620.hw3.Constants.YEAR;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightJoin {

  public static class FlightMapper extends Mapper<Object, Text, Text, Text> {

    private final FlightInfoParser flightInfoParser;
    private final FlightFilter flightFilter;

    public FlightMapper() {
      this.flightInfoParser = new FlightInfoParser();
      this.flightFilter = new FlightFilter(2007, 6, 2008, 5);
    }

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      String line = value.toString();
      Map<String, String> fieldsMap = flightInfoParser.parseLine(line);
      if (flightFilter.isValid(fieldsMap)) {
        context.write(
            ORD.equalsIgnoreCase(fieldsMap.get(ORIGIN)) ? // Use connection airport as key
                new Text(fieldsMap.get(DEST)) : new Text(fieldsMap.get(ORIGIN)),
            flightInfoParser.mapComposer(fieldsMap));
      }
    }
  }

  public static class FlightPartitioner extends Partitioner<Text, Text> {

    private final FlightInfoParser flightInfoParser = new FlightInfoParser();

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
      Map<String, String> valueMap = flightInfoParser.decomposeIntermediateText(value);
      return Integer.parseInt(valueMap.get(MONTH)) % numPartitions;
    }

  }

  public static class FlightReducer extends Reducer<Text, Text, IntWritable, DoubleWritable> {

    private final FlightInfoParser flightInfoParser = new FlightInfoParser();
    private double totalDelay = 0;
    private int count = 0;

    @Override
    public void reduce(Text key, Iterable<Text> textValues, Context context)
        throws IOException, InterruptedException {
      List<Map<String, String>> ordFlights = new ArrayList<>();
      List<Map<String, String>> jfkFlights = new ArrayList<>();


      // Separate ORD and JFK flights
      for (Text textValue : textValues) {
        Map<String, String> value = flightInfoParser.decomposeIntermediateText(textValue);
        String departureAirport = value.get(ORIGIN);
        String arrivalAirport = value.get(DEST);
        if (ORD.equalsIgnoreCase(departureAirport)) {
          ordFlights.add(value);
        } else if (JFK.equalsIgnoreCase(arrivalAirport)) {
          jfkFlights.add(value);
        } else {
          System.out.println(
              "Unexpected flight: departure-" + departureAirport + " | arrival-" + arrivalAirport
                  + " | time:" + value.get(YEAR) + "-" + value.get(MONTH));
        }
      }

      // Join two leg flights
      for (Map<String, String> ordFlight : ordFlights) {
        String arrDateORD = ordFlight.getOrDefault(FLIGHT_DATE, "");
        String arrTimeORD = ordFlight.get(ARR_TIME);
        double delayORD = Double.parseDouble(ordFlight.get(ARR_DELAY_MINUTES));

        for (Map<String, String> jfkFlight : jfkFlights) {
          String depDateJFK = jfkFlight.getOrDefault(FLIGHT_DATE, "");
          if (!arrDateORD.equalsIgnoreCase(depDateJFK)) {
            continue;
          }
          String depTimeJFK = jfkFlight.get(DEP_TIME);

          if (depTimeJFK.compareTo(arrTimeORD)>0) {
            double delayJFK = Double.parseDouble(jfkFlight.get(ARR_DELAY_MINUTES));
            totalDelay += delayORD + delayJFK;
            count++;
          }
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new IntWritable(count), new DoubleWritable(totalDelay));
    }
  }

  public static class AvgMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try{
        if(!Strings.isNullOrEmpty(value.toString())){
          context.write(new Text("Finalized"), value);
        }
      }catch (Exception e){
        e.printStackTrace();
      }
    }
  }

  public static class AvgReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    private int totalCount = 0;
    private double totalDelay = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      try{
        for(Text line: values){
          String[] fields = line.toString().split("\t");
          totalCount+=Integer.parseInt(fields[0]);
          totalDelay+=Double.parseDouble(fields[1]);
        }
      }catch (Exception e){
        e.printStackTrace();
      }
    }

    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      context.write(new Text("Overall Average Delay"), new DoubleWritable(totalDelay/totalCount));
    }
  }


    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "flight join");
    job.setJarByClass(FlightJoin.class);
    job.setMapperClass(FlightMapper.class);
    job.setReducerClass(FlightReducer.class);
    job.setPartitionerClass(FlightPartitioner.class);
    job.setNumReduceTasks(10);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);

    //calculate average
    job = Job.getInstance(conf, "compute avg delay");
    job.setJarByClass(FlightJoin.class);
    job.setMapperClass(AvgMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(AvgReducer.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}