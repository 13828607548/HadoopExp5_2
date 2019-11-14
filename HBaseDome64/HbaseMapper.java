package HBaseDome64;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HbaseMapper  extends TableMapper<Text,IntWritable> {
    @Override
    protected void map
        (ImmutableBytesWritable key, Result value, Context context)
    throws IOException,InterruptedException {
        /**
         *key相当于行键
         *value 一行记录
         */
        String city = Bytes.toString(value.getValue(Bytes.toBytes("address"),Bytes.toBytes("city")));

        context.write(new Text(city),new IntWritable(1));


    }
}
