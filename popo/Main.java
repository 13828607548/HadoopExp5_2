package popo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Main {
    public static void main( String[] args ) throws Exception {
        BasicConfigurator.configure();
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Main.class);
        //指定job的mapper和输出的类型<k2,v2>,单独开一个线程运行
        job.setMapperClass(SogouMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mysogou.class);
        //指定分区规则
        job.setPartitionerClass(SogouParitioner.class);
        job.setNumReduceTasks(2);
        //指定job的reducer和输出的类型<k4,v4>,单独开一个线程运行
        job.setReducerClass(SogouReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //指定job的输入和输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //开始执行任务
        job.waitForCompletion(true);
    }
}

