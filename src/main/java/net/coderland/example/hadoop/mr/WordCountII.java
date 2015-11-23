package net.coderland.example.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Author: zhangxin
 * Date:   15-11-23
 */
public class WordCountII {

    public static class WordCountMapperII extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class WordCountReducerII extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while(values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://ubuntu-zhangxin:9000/sandbox/wordcount";
        String output = "hdfs://ubuntu-zhangxin:9000/sandbox/wordcount/output";

        JobConf conf = new JobConf(WordCountII.class);
        //设置job的名字
        conf.setJobName("WordCount");

        //为job指定hadoop的core-site.xml, hdfs-site.xml, mapred-site.xml等三个配置文件的路径。
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        //设置output的key-value的类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        //设置map, combiner, reducer的类
        conf.setMapperClass(WordCountMapperII.class);
        conf.setCombinerClass(WordCountReducerII.class);
        conf.setReducerClass(WordCountReducerII.class);

        //设置输入和输出的格式
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //设置输入源的路径以及结果的输出路径
        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        //提交job
        JobClient.runJob(conf);
        System.exit(0);
    }
}
