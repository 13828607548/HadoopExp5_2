package HBaseDome64;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
public class HbaseMain {
    public static void main (String[] args)throws Exception{
        BasicConfigurator.configure();
        //从Hbase中获取数据
        Configuration conf = new Configuration() ;
        conf.set("hbase.zookeeper.quorum","192.168.30.131");
        //创建任务

        Job job =Job.getInstance(conf);
        job.setJarByClass(HbaseMain.class);
        //定义一个扫描器读取要处理的数据
        Scan scan = new Scan();
        //指定扫描器扫描的数据
        scan.addColumn(Bytes.toBytes("address"),Bytes.toBytes("city"));
        //指定Map输出表 member241
        TableMapReduceUtil.initTableMapperJob("member241", scan,
                HbaseMapper.class,Text.class, IntWritable.class,job);
        //指定Reduce输出的表 result241
        TableMapReduceUtil.initTableReducerJob("result241",HbaseReducer.class,job);
        job.waitForCompletion(true);
    }
}
