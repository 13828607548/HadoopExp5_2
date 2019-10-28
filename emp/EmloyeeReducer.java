package emp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EmloyeeReducer extends Reducer<IntWritable,Employee,IntWritable,Employee> {

    @Override
    protected void reduce(IntWritable k3, Iterable<Employee> v3, Context context)
            throws IOException, InterruptedException {
        int total = 0;
        for (Employee e : v3) {
            context.write(k3, e);
        }
    }
}