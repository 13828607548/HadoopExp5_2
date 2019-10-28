package sales;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Salary implements Writable{
    private int prod_id;          //产品ID
    private int cust_id;         //客户ID
    private String time;         //日期
    private int channel_id;       //渠道ID
    private int promo_id;         //促销ID
    private int quantity_sold;    //销售数量
    private double amount_sold;    //销售总额
    public Salary(){}
    public Salary(int quantity_sold,double amount_sold){
        this.quantity_sold = quantity_sold;
        this.amount_sold = amount_sold;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //序列化
        dataOutput.writeInt(this.prod_id);
        dataOutput.writeInt(this.cust_id);
        dataOutput.writeUTF(this.time);
        dataOutput.writeInt(this.channel_id);
        dataOutput.writeInt(this.promo_id);
        dataOutput.writeInt(this.quantity_sold);
        dataOutput.writeDouble(amount_sold);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //反序列化
        this.prod_id = dataInput.readInt();
        this.cust_id = dataInput.readInt();
        this.time = dataInput.readUTF();
        this.channel_id = dataInput.readInt();
        this.promo_id = dataInput.readInt();
        this.quantity_sold = dataInput.readInt();
        this.amount_sold = dataInput.readInt();
    }

    public int getProd_id() {
        return prod_id;
    }

    public void setProd_id(int prod_id) {
        this.prod_id = prod_id;
    }

    public int getCust_id() {
        return cust_id;
    }

    public void setCust_id(int cust_id) {
        this.cust_id = cust_id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getChannel_id() {
        return channel_id;
    }

    public void setChannel_id(int channel_id) {
        this.channel_id = channel_id;
    }

    public int getPromo_id() {
        return promo_id;
    }

    public void setPromo_id(int promo_id) {
        this.promo_id = promo_id;
    }

    public int getQuantity_sold() {
        return quantity_sold;
    }

    public void setQuantity_sold(int quantity_sold) {
        this.quantity_sold = quantity_sold;
    }

    public double getAmount_sold() {
        return amount_sold;
    }

    public void setAmount_sold(double amount_sold) {
        this.amount_sold = amount_sold;
    }

    @Override
    public String toString() {
        return  "销售数量为" + quantity_sold +","+ " 销售总额为" + amount_sold;
    }
}
