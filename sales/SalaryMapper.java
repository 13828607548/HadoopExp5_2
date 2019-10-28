package sales;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalaryMapper extends Mapper<LongWritable, Text, IntWritable, Salary> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
        //数据:13，	987，(1998-1-10)，3，999，1，(1232.16)
        String data = value1.toString();
        String[] words = data.split(",");
        //创建销售记录
        Salary salary = new Salary();
        //产品id
        salary.setProd_id(Integer.parseInt(words[0]));
        //客户id
        salary.setCust_id(Integer.parseInt(words[1]));
        //日期
        salary.setTime(words[2]);
        String[] time = words[2].split("-");
        //渠道id
        salary.setChannel_id(Integer.parseInt(words[3]));
        //促销id
        salary.setPromo_id(Integer.parseInt(words[4]));
        //销售数量
        salary.setQuantity_sold(Integer.parseInt(words[5]));
        //销售总额
        salary.setAmount_sold(Double.parseDouble(words[6]));
        //输出            k2 产品id                               v2销售对象
        context.write(new IntWritable(Integer.parseInt(time[0])),salary);
    }
}
