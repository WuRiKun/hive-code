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

/*5.10查询搜索过”仙剑奇侠传“的uid，并且次数大于3*/
public class QuerySearch extends Configured implements Tool {

    public static class QuerySearchMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text();
        private LongWritable ovalue=new LongWritable(1L);
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            String uid=lineSplited[1];
            String keyword=lineSplited[2];
            if(keyword.equals("仙剑奇侠传")) {
                String uid_keyword=uid+"_"+keyword;
                okey.set(uid_keyword);
                context.write(okey, ovalue);
            }
        }
    }

    public static class QuerySearchReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        private LongWritable ovalue=new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum =0;
            for(LongWritable value:values) {
                sum +=value.get();
            }
            if(sum > 3) {
                ovalue.set(sum);
                context.write(key, ovalue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.49.23.127:9000");

        Job job=Job.getInstance(conf);
        job.setJarByClass(QuerySearch.class);
        FileInputFormat.addInputPath(job, new Path("/sougou/sogou_log.txt.flt"));

        job.setMapperClass(QuerySearchMapper.class);
        job.setReducerClass(QuerySearchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path("/output/10_QuerySearch"));
        return job.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new QuerySearch(), args);
        System.exit(res);
    }

}