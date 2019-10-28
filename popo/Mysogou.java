package popo;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Mysogou implements Writable {
    private String time;
    private String number;
    private String name;
    private int URL_rank;       //URL地址在搜索结果中的排名
    private int User_click;      //用户在搜索的URL地址中点击的顺序
    private String url;

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getNumber(){
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getName(){
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getURL_rank() {
        return URL_rank;
    }

    public void setURL_rank(int URL_rank) {
        this.URL_rank = URL_rank;
    }

    public int getUser_click(){
        return User_click;
    }

    public void setUser_click(int User_click) {
        this.User_click = User_click;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public void write(DataOutput output) throws IOException {   //序列化
        output.writeUTF(this.time);
        output.writeUTF(this.number);
        output.writeUTF(this.name);
        output.writeInt(this.URL_rank);
        output.writeInt(this.User_click);
        output.writeUTF(this.url);
    }

    @Override
    public void readFields(DataInput input) throws IOException {   //反序列化
        this.time = input.readUTF();
        this.number = input.readUTF();
        this.name = input.readUTF();
        this.URL_rank = input.readInt();
        this.User_click = input.readInt();
        this.url = input.readUTF();
    }
}
