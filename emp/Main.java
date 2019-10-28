package emp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static  void  main(String[] args) throws Exception{

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Main.class);

        job.setMapperClass(EmployeeMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Employee.class);

        //指定任务的分区规则
        job.setPartitionerClass(EmployeeParitioner.class);
        job.setNumReduceTasks(3);

        //指定job的reducer和输出类型
        job.setReducerClass(EmloyeeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Employee.class);

        //指定job的输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //执行任务

        job.waitForCompletion(true);
    }
}
