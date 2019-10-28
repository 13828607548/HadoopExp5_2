package wc;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text,IntWritable,Text, IntWritable> {
    @Override
    protected void reduce (Text k3 , Iterable<IntWritable> v3 ,Context context ) throws IOException,
            InterruptedException{
     /*
     * context 是reduce的上下文
     * 上文
     * 下文
     * */
     //对v3求和
        int total = 0;
        for (IntWritable v:v3){
            total += v.get();
        }
        // 输出 ；  K4单词 v4  频率
        context.write(k3, new IntWritable(total));
    }
}
