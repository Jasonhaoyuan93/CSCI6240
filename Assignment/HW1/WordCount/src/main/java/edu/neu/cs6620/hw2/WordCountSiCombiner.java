package edu.neu.cs6620.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountSiCombiner {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private final Text word;
    private final ValidatorAndPartitioner validatorAndPartitioner;

    public TokenizerMapper(){
      this.word = new Text();
      this.validatorAndPartitioner = new ValidatorAndPartitioner();
    }

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String curToken = itr.nextToken();
        if(validatorAndPartitioner.isValid(curToken)){
          word.set(curToken);
          context.write(word, one);
        }
      }
    }
  }

  public static class IntSumReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {
    private final IntWritable result;
    public IntSumReducer(){
      this.result = new IntWritable();
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class IntPartitioner extends Partitioner<Text, IntWritable>{
    private final ValidatorAndPartitioner validatorAndPartitioner;

    public IntPartitioner(){
      this.validatorAndPartitioner = new ValidatorAndPartitioner();
    }

    /**
     * Get the partition number for a given key (hence record) given the total
     * number of partitions i.e. number of reduce-tasks for the job.
     *
     * @param text          the key to be partioned.
     * @param intWritable   the entry value.
     * @param numPartitions the total number of partitions.
     * @return the partition number for the <code>key</code>.
     */
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
      return validatorAndPartitioner.partition(text.toString());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountSiCombiner.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setNumReduceTasks(5);
    job.setPartitionerClass(IntPartitioner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}