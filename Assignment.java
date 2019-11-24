package com.mr.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.*;

public class Assignment {

    static class Map extends Mapper <LongWritable, Text,Text,IntWritable>{
        private TreeMap<Integer,String> tm;

        @Override
        public void setup (Context ctx){
            tm = new TreeMap<Integer,String>();
        }

        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String productID = value.toString().split(",")[1];
            String action =value.toString().split(",")[2];

            if(action.equalsIgnoreCase("Purchase")){
                tm.put(1,productID);
            }

            if (tm.size() >10)
                tm.remove(tm.firstKey());
        }

        @Override
        public void cleanup (Context ctx) throws IOException,InterruptedException{
            for (Entry<Integer,String> entry :tm.entrySet()){
                int count = entry.getKey();
                String productId = entry.getValue();

                ctx.write(new Text(productId),new IntWritable(count));

            }
        }
    }
    static class Reduce extends Reducer <Text,IntWritable,Text,IntWritable>{

        private TreeMap<Integer,String> tm2;

        @Override
        public void setup (Context ctx){
            tm2 = new TreeMap<Integer,String>();
        }

        public void reduce (Text key,Iterator<IntWritable> values,Context ctx) throws IOException,InterruptedException{
            String productId = key.toString();
            int sumOfPurchase = 0;

            while (values.hasNext()){
                sumOfPurchase +=values.next().get();
            }

            tm2.put(sumOfPurchase,productId);

            if (tm2.size()>10)
                tm2.remove(tm2.firstKey());

        }

        @Override
        public void cleanup(Context ctx) throws InterruptedException,IOException{
            for (Entry<Integer,String> e : tm2.entrySet()){
                String productId = e.getValue();
                int sumOfPurchase = e.getKey();

                ctx.write(new Text(productId),new IntWritable(sumOfPurchase));
            }
        }
    }

    public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        if (otherArgs.length <2 ){
            System.err.println("Please provide input and output path:");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf,"Assignment");

        job.setJarByClass(Assignment.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }

}
