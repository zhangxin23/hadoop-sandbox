package net.coderland.example.hadoop.mr.kpi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Author: zhangxin
 * Date:   15-11-25
 */
public class KPIRemoteAddr {

    public static class KPIRemoteAddrMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.parser(value.toString());
            if(kpi.isValid()) {
                word.set(kpi.getRemote_addr());
                output.collect(word, one);
            }
        }
    }

    public static class KPIRemoteAdderReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while(values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        String input = "hdfs://ubuntu-zhangxin:9000/sandbox/log_kpi";
        String output = "hdfs://ubuntu-zhangxin:9000/sandbox/log_kpi/remote_addr";

        JobConf conf = new JobConf(KPIRemoteAddr.class);
        conf.setJobName("KPIRemoteAddr");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(KPIRemoteAddrMapper.class);
        conf.setCombinerClass(KPIRemoteAdderReduce.class);
        conf.setReducerClass(KPIRemoteAdderReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }
}
