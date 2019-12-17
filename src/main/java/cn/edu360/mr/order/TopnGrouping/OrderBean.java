package cn.edu360.mr.order.TopnGrouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private String userId;
    private String pdtName;
    private float price;
    private int number;
    private float amount;

    public void set(String orderId, String userId, String pdtName, float price, int number) {
        this.orderId = orderId;
        this.userId = userId;
        this.pdtName = pdtName;
        this.price = price;
        this.number = number;
        this.amount = this.price * this.number;
    }

    @Override
    public String toString() {
        return this.orderId+","+this.userId+","+this.pdtName+","+this.price+","+this.number+","+this.amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPdtName() {
        return pdtName;
    }

    public void setPdtName(String pdtName) {
        this.pdtName = pdtName;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.userId);
        out.writeUTF(this.pdtName);
        out.writeFloat(this.price);
        out.writeInt(this.number);
        out.writeFloat(this.amount);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.userId = in.readUTF();
        this.pdtName = in.readUTF();
        this.price = in.readFloat();
        this.number = in.readInt();
        this.amount = this.price* this.number;

    }


    @Override
    public int compareTo(OrderBean o) {

//       float tmp = o.getAmount() - this.getAmount();
//       if(tmp == 0){
//           return this.pdtName.compareTo(o.getPdtName());
//       }
//       else if(tmp <0){
//           return 1;
//       }
//       else{
//           return -1;
//       }


        return this.orderId.compareTo(o.getOrderId())==0?Float.compare(o.getAmount(),this.getAmount()):this.orderId.compareTo(o.getOrderId());



    }
}
