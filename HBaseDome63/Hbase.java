package HBaseDome63;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;



public class Hbase {
    //创建表
    private static void createTable() throws Exception {
        //指定ZooKeeper地址，从zk中获取HMaster的地址
        //注意：ZK返回的是HMaster的主机名, 不是IP地址 ---> 配置Windows的hosts文件

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.30.131");

        //创建一个HBase的客户端
        HBaseAdmin client = new HBaseAdmin(conf);

        //创建表: 通过表的描述符
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("member241"));

        //列族的信息
        HColumnDescriptor h1 = new HColumnDescriptor("info");
        HColumnDescriptor h2 = new HColumnDescriptor("address");
        HColumnDescriptor h3 = new HColumnDescriptor("id");

        //将列族加入表
        htd.addFamily(h1);
        htd.addFamily(h2);
        htd.addFamily(h3);
        //创建表
        client.createTable(htd);
        System.out.print("创建表成功");
        client.close();
    }

    //增加记录
    public static void insert() throws Exception {
        //指定的配置信息: ZooKeeper
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.30.131");

        //客户端
        HTable table = new HTable(conf, "member241");

        //第一条数据
        Put put1 = new Put(Bytes.toBytes("Rain"));
        put1.add(Bytes.toBytes("id"), Bytes.toBytes(" "), Bytes.toBytes("31"));

        //第二条数据
        Put put2 = new Put(Bytes.toBytes("Rain"));
        put2.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("28"));

        //第三条数据
        Put put3 = new Put(Bytes.toBytes("Rain"));
        put3.add(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1990-05-01"));

        //第四条数据
        Put put4 = new Put(Bytes.toBytes("Rain"));
        put4.add(Bytes.toBytes("info"), Bytes.toBytes("industry"), Bytes.toBytes("architect"));

        //第五条数据
        Put put5 = new Put(Bytes.toBytes("Rain"));
        put5.add(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("ShenZhen"));

        //第六条数据
        Put put6 = new Put(Bytes.toBytes("Rain"));
        put6.add(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("China"));


        //构造List
        List<Put> list = new ArrayList<Put>();
        list.add(put1);
        list.add(put2);
        list.add(put3);
        list.add(put4);
        list.add(put5);
        list.add(put6);

        //插入数据
        table.put(list);
        System.out.print("数据插入成功");
        table.close();
    }

    //获取行某列族
    private static void get() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.30.131");

        //指定表的客户端
        HTable table = new HTable(conf, "member241");

        //通过Get查询
        Get get = new Get(Bytes.toBytes("Rain"));
        get.addFamily(Bytes.toBytes("info"));
        //执行查询
        Result result = table.get(get);
        List<KeyValue> name = result.list();
        //循环输出
        for (KeyValue v:name){
            System.out.println(Bytes.toString(v.getFamily())+":"+Bytes.toString(v.getQualifier())+",timestamp="
                    +Long.toString(v.getTimestamp())+",value="+Bytes.toString(v.getValue()));
            //输出列族名，列族内的列名，时间戳，列的值
        }
        table.close();
    }

    //扫描某列
    private static void scan() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.30.131");

        //指定表的客户端
        HTable table = new HTable(conf, "member241");

        //创建一个扫描器 Scan
        Scan scanner = new Scan(); //----> 相当于: select * from students;
        //scanner.setFilter(filter)  ----> 过滤器

        //执行查询
        ResultScanner rs = table.getScanner(scanner); //返回ScannerResult ---> Oracle中的游标
        for (Result r : rs) {
            String row = Bytes.toString(r.getRow());
            //输出行名
            System.out.print("ROW="+row+" ");
            List<KeyValue> family = r.getColumn(Bytes.toBytes("info"),Bytes.toBytes("birthday"));
            for (KeyValue v:family){
                System.out.print(Bytes.toString(v.getFamily())+":"+Bytes.toString(v.getQualifier())+",timestamp="
                        +Long.toString(v.getTimestamp()));
            }
            String birthday = Bytes.toString(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("birthday")));
            System.out.println(",value="+birthday);
        }
        table.close();
    }


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
           createTable();
           insert();
           get();
           scan();
    }
}
