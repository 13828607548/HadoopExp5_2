package emp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * 建立自己的分区规则，根据员工的工资高低进行分区
 * 根据MAP的输出  k2 v2
 */

    public class EmployeeParitioner extends Partitioner<IntWritable,Employee> {
        @Override
        public int getPartition(IntWritable k2, Employee v2, int numPartition) {
            if (v2.getSal() < 1500){
                return 1 % numPartition;
            }else if (v2.getSal()>=1500 && v2.getSal() <3000) {
                return 2 % numPartition;
            }else {
                return 3 % numPartition;
            }
        }
    }
