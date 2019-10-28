package popo;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class SogouParitioner extends Partitioner<Text,Mysogou> {
    @Override
    public int getPartition(Text text, Mysogou data, int numPartitions) {
        //分区
        if (data.getURL_rank() == 2 && data.getUser_click() == 1){
            //1号区
            return 1/numPartitions;
        }
        else {  //2号区
            return 2/numPartitions;
        }
    }
}