package com.sogou;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*查询次数大于2次的用户占比*/
public class RatioOfQueriesGreaterThan2 extends Configured implements Tool {

    public static class UserDutyThanTwoMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        private Text okey=new Text("userDutyThanTwn");
        private LongWritable ovalue=new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            long count=Long.parseLong(lineSplited[1]);
            ovalue.set(count);
            context.write(okey, ovalue);
        }
    }

    public static class UserDutyThanTwoReducere extends Reducer<Text, LongWritable, Text, DoubleWritable>{
        private Text okey=new Text("userDutyThanTwn");
        private DoubleWritable percent=new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuffer buffer=new StringBuffer();
            for(LongWritable value:values) {
                buffer.append(value).append(",");
            }
            String[] moleculeOrDenominator=buffer.toString().split(",");
            double a=Double.valueOf(moleculeOrDenominator[0]);
            double b=Double.valueOf(moleculeOrDenominator[1]);
            double per=0.0;
            if(a<=b) {
                per=a/b;
            }else {
                per=b/a;
            }
            percent.set(per);
            context.write(okey, percent);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
         conf.set("fs.defaultFS", "hdfs://mini1:9000");
        Job job1=Job.getInstance(conf);

        job1.setJarByClass(RatioOfQueriesGreaterThan2.class);
        MultipleInputs.addInputPath(job1, new Path("/output/4_IndependentUID"),
                TextInputFormat.class, UserDutyThanTwoMapper.class);
        MultipleInputs.addInputPath(job1, new Path("/output/6_QueriesGreaterThan2"),
                TextInputFormat.class, UserDutyThanTwoMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setReducerClass(UserDutyThanTwoReducere.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileOutputFormat.setOutputPath(job1, new Path("/output/7_RatioOfQueriesGreaterThan2"));
        return job1.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new RatioOfQueriesGreaterThan2(), args);
        System.exit(res);
    }

}
