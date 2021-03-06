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


public class NotRepeatQueryTotalNumber extends Configured implements Tool {

    public static class NotRepeatQueryTotalNumberMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text();
        private LongWritable ovalue=new LongWritable(1L);
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuffer sb=new StringBuffer();
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            sb.append(lineSplited[0]).append("_")
                    .append(lineSplited[1]).append("_")
                    .append(lineSplited[2]).append("_")
                    .append(lineSplited[5]);

            okey.set(sb.toString());
            context.write(okey, ovalue);
        }
    }
    public static class NotRepeatQueryTotalNumberReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
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

    public static class NotRepeatQueryTotalNumberMapper2 extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text("NotRepeatQueryTotalNumber");
        private LongWritable ovalue=new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] splited=value.toString().split("\t");
            long count=Long.valueOf(splited[1]);
            if(count==1) {
                ovalue.set(count);
                context.write(okey, ovalue);
            }

        }
    }
    public static class NotRepeatQueryTotalNumberReducer2 extends Reducer<Text, LongWritable, Text, LongWritable>{
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

        job1.setJarByClass(NotRepeatQueryTotalNumber.class);
        FileInputFormat.addInputPath(job1, new Path("/sougou/sogou_log.txt.flt"));
        job1.setMapperClass(NotRepeatQueryTotalNumberMapper.class);
        job1.setReducerClass(NotRepeatQueryTotalNumberReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job1, new Path("/outdata/sogou_notrepeat"));
        job1.waitForCompletion(true);

        Job job2=Job.getInstance(conf);

        job2.setJarByClass(NotRepeatQueryTotalNumber.class);
        FileInputFormat.addInputPath(job2, new Path("/outdata/sogou_notrepeat"));
        job2.setMapperClass(NotRepeatQueryTotalNumberMapper2.class);
        job2.setReducerClass(NotRepeatQueryTotalNumberReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path("/output/3_NotRepeatQueryTotalNumber"));
        return job2.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new NotRepeatQueryTotalNumber(), args);
        System.exit(res);
    }

}