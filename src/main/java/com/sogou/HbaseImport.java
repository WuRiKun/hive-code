package com.sogou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/*将生成的文件通过Java API方式导入到HBase（一张表）*/
public class HbaseImport {

    // reduce输出的表名
    // private static String tableName = "sogou_data_analysis_results_table";
    private static String tableName = "sogou_data";
    // 初始化连接

    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
         conf.set("fs.defaultFS", "hdfs://mini1:9000");
        conf.set("hbase.master", "hdfs://10.49.23.127:60000");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "10.49.23.127,10.49.23.134,10.49.23.129");
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
//		conf.set("dfs.socket.timeout", "180000");
    }

    public static class BatchMapper extends
            Mapper<LongWritable, Text, LongWritable, Text> {
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            Text v2s = new Text();
            v2s.set(line);
            context.write(key, v2s);
        }
    }

    public static class BatchReducer extends
            TableReducer<LongWritable, Text, NullWritable> {
        private String family = "info";

        @Override
        protected void reduce(
                LongWritable arg0,
                Iterable<Text> v2s,
                Reducer<LongWritable, Text, NullWritable, Mutation>.Context context)
                throws IOException, InterruptedException {
            for (Text v2 : v2s) {
                String[] splited = v2.toString().split("\t");
                String rowKey = splited[0];
                Put put = new Put(rowKey.getBytes());
//				put.addColumn(family.getBytes(), "raw".getBytes(), v2.toString().getBytes());

                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("raw"), Bytes.toBytes(v2.toString()));
                context.write(NullWritable.get(), put);
            }
//			for (Text v2 : v2s) {
//				String[] splited = v2.toString().split("\t");
//				String rowKey = splited[0];
//				Put put = new Put(Bytes.toBytes("rowkey"));
////				put.addColumn(family.getBytes(), "raw".getBytes(), v2.toString().getBytes());
//
//				put.addColumn(Bytes.toString(family), Bytes.toBytes("raw"), Bytes.toBytes(v2.toString()));
//				context.write(NullWritable.get(), put);
//		}
        }

        public static void imputil(String str) throws IOException, ClassNotFoundException,
                InterruptedException {
            Job job = Job.getInstance(conf, HbaseImport.class.getSimpleName());
            TableMapReduceUtil.addDependencyJars(job);
            job.setJarByClass(HbaseImport.class);
            FileInputFormat.setInputPaths(job,str);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(BatchMapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(BatchReducer.class);
            job.setOutputFormatClass(TableOutputFormat.class);
            job.waitForCompletion(true);
        }

        public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
            String[] str={
                    "hdfs://10.49.23.127:9000/output/1_QueryTotalNumber",
                    "hdfs://10.49.23.127:9000/output/2_NotNullQueryTotalNumber",
                    "hdfs://10.49.23.127:9000/output/3_NotRepeatQueryTotalNumber",
                    "hdfs://10.49.23.127:9000/output/4_IndependentUID",
                    "hdfs://10.49.23.127:9000/output/5_QueryFreRankTop50",
                    "hdfs://10.49.23.127:9000/output/6_QueriesGreaterThan2",
                    "hdfs://10.49.23.127:9000/output/7_RatioOfQueriesGreaterThan2",
                    "hdfs://10.49.23.127:9000/output/8_RatioOfClickTimesInTen",
                    "hdfs://10.49.23.127:9000/output/9_RatioOfDirectInputURL",
                    "hdfs://10.49.23.127:9000/output/10_QuerySearch"};
            for	(String stri:str){
                imputil(stri);
            }
        }
    }
}