package popo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SogouReducer extends Reducer<Text,Mysogou,Text,Text> {
    @Override
    protected void reduce(Text k3,Iterable<Mysogou> v3,Context context) throws IOException, InterruptedException {
        for (Mysogou d : v3){
            String str = "URL 排名:" + d.getURL_rank() + "    用户点击顺序:" + d.getUser_click();
            String str1 = d.getName() + "   " + d.getUrl();
            context.write(new Text(str),new Text(str1));
        }
    }
}