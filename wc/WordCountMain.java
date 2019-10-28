package wc;
import  java.io.IOException;

import  org.apache.hadoop.conf.Configuration;
import  org.apache.hadoop.fs.Path;

import  org.apache.hadoop.io.Text;
import  org.apache.hadoop.io.IntWritable;

import  org.apache.hadoop.mapreduce.Job;
import  org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import  org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
public class WordCountMain {
public static void main(String [] args) throws Exception {
    BasicConfigurator.configure();
    Job job =Job.getInstance(new Configuration());
    job.setJarByClass(WordCountMain.class);
    //指定job的mapper和输出的类型<k2 v2>
    job.setMapperClass(WordCountMapper.class);
    job.setMapOutputKeyClass(Text.class);//k2
    job.setMapOutputValueClass(IntWritable.class);//v2
    //指定job的reducer和输出的类型<k4  v4>
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);//k4
    job.setOutputValueClass(IntWritable.class);//v4
    //引入job的combiner和输出类型<k3  v3>
    job.setCombinerClass(WordCountReducer.class);
    //指定job的输入和输出
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //执行job
    job.waitForCompletion(true);

    }
  }
