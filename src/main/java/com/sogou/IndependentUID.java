package com.sogou;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IndependentUID extends Configured implements Tool {

    public static class IndependentUIDMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text();
        private LongWritable ovalue=new LongWritable(1L);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            String uid=lineSplited[1];
            if(!"".equals(uid) || uid!=null) {
                okey.set(uid);
                context.write(okey, ovalue);
            }

        }
    }

    public static class IndependentUIDReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        private LongWritable ovalue=new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            for(LongWritable value:values) {
                sum +=value.get();
            }
            ovalue.set(sum);
            context.write(key, ovalue);
        }
    }

    public static class IndependentUIDMapper2 extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text("independentUID");
        private LongWritable ovalue=new LongWritable(1L);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            long count=Long.valueOf(lineSplited[1]);
            if(count >=1) {
                context.write(okey, ovalue);
            }

        }
    }

    public static class IndependentUIDReducer2 extends Reducer<Text, LongWritable, Text, LongWritable>{
        private LongWritable ovalue=new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            for(LongWritable value:values) {
                sum +=value.get();
            }
            ovalue.set(sum);
            context.write(key, ovalue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://mini1:9000");
        Job job1=Job.getInstance(conf);

        job1.setJarByClass(IndependentUID.class);
        FileInputFormat.addInputPath(job1, new Path("/sougou/sogou_log.txt.flt"));
        job1.setMapperClass(IndependentUIDMapper.class);
        job1.setReducerClass(IndependentUIDReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job1, new Path("/outdata/sogou_independentUID"));
        job1.waitForCompletion(true);

        Job job2=Job.getInstance(conf);

        job2.setJarByClass(IndependentUID.class);
        FileInputFormat.addInputPath(job2, new Path("/outdata/sogou_independentUID"));
        job2.setMapperClass(IndependentUIDMapper2.class);
        job2.setReducerClass(IndependentUIDReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path("/output/4_IndependentUID"));
        return job2.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new IndependentUID(), args);
        System.exit(res);
    }
}