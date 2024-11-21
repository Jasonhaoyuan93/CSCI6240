package edu.neu.cs6620.hw4;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

import static edu.neu.cs6620.hw4.Constants.*;

public class HPopulateDriver {
    private static final String TABLE_NAME = "FlightData";
    private static final String COL_INFO_NAME = "info";
    public static class HPopulateMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try{
                String[] fields = value.toString().split(",(?=\\S)");
//                String airline = fields[6]; // carrier
                String airline = fields[7]; // airlineId
                //use airline ID as key to group all records in the same airline into one reducer
                context.write(new Text(airline), new Text(value));
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }
    public static class HPopulateReducer extends Reducer<Text, Text, Text, LongWritable> {

        private final Connection connection;
        private final Table table;
        private final FlightInfoParser flightInfoParser;
        private final int batchSize;
        private final Map<String, Long> countMap;

        public HPopulateReducer() throws IOException {

            Configuration conf = HBaseConfiguration.create();;
            if (hBaseSite !=null){
                conf.addResource(new File(hBaseSite).toURI().toURL());
            }
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            flightInfoParser = new FlightInfoParser(new HardCodeCSVProvider());
            batchSize = 5000;
            countMap = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try{
                List<Put> puts = new LinkedList<>();
                int batchLeft = this.batchSize;
                for(Text value: values){
                    batchLeft--;
                    Map<String, String> fields = flightInfoParser.parseLine(value.toString());
                    String airline = fields.get(CARRIER);
                    String year = fields.get(YEAR);
                    String month = fields.get(MONTH);
                    // Create HBase row key: Airline + Year + Month auto increment index
                    Put put = new Put((airline+"-"+year+"-"+month).getBytes());
                    for(Map.Entry<String,String> row: fields.entrySet()){
                        if(row!=null&&row.getKey()!=null&&StringUtils.isNotBlank(row.getValue())){
                            put.addColumn(COL_INFO_NAME.getBytes(), row.getKey().getBytes(), row.getValue().getBytes());
                        }
                    }
                    puts.add(put);
                    if(batchLeft<=0){
                        table.put(puts);
                        batchLeft=this.batchSize;
                        puts = new LinkedList<>();
                    }
                }
                if(puts.size()>0){
                    table.put(puts);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try{
                if (table != null ) {
                    table.close();
                }
                if (connection != null) connection.close();

                //write all records from same airline with different years
                for(Map.Entry<String,Long> entry:countMap.entrySet()){
                    context.write(new Text(entry.getKey()),new LongWritable(entry.getValue()));
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    public static class AirlinePartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.toString().hashCode()%numPartitions;
        }
    }

    public static String hBaseSite;

    public static void main(String[] args) throws Exception {
        try{

            //create 'FlightData' table if not exist
            Configuration conf = HBaseConfiguration.create();
            try(Connection connection = ConnectionFactory.createConnection(conf);
                Admin hBaseAdmin = connection.getAdmin()){
                if(!hBaseAdmin.tableExists(TableName.valueOf(TABLE_NAME))){
                    TableDescriptorBuilder tBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
                    ColumnFamilyDescriptor infoDesc = ColumnFamilyDescriptorBuilder.newBuilder(COL_INFO_NAME.getBytes()).build();
                    TableDescriptor tDesc = tBuilder.setColumnFamily(infoDesc).build();
                    hBaseAdmin.createTable(tDesc);
                }
            }
            conf = new Configuration();
            Job job = Job.getInstance(conf, "H-POPULATE");

            job.setJarByClass(HPopulateDriver.class);
            job.setMapperClass(HPopulateMapper.class);
            job.setReducerClass(HPopulateReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setPartitionerClass(AirlinePartitioner.class);
            job.setNumReduceTasks(10);

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
