package sales;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalaryReducer extends Reducer<IntWritable, Salary, IntWritable,Salary> {
    @Override
    protected void reduce(IntWritable k3, Iterable<Salary> v3, Context context) throws IOException, InterruptedException {
        //输入k3会有一条对应的销售记录
        int  total_quantity = 0;
        double total_amount = 0.0;
        for (Salary s:v3){
            total_quantity += s.getQuantity_sold();
            total_amount += s.getAmount_sold();
        }
        //输出        年份                  数量及总额
        context.write(k3,new Salary(total_quantity,total_amount));
    }
}

