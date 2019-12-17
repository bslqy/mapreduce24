package cn.edu360.mr.flowProvince;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义类型如何输出Hadoop的序列化接口
 * 1. 一定要有保留空参构造函数
 * 2. write方法中输出字段二进制数据顺序要与readFields方法读取数据顺序要一样
 * 3. Reducer 重载toString() 方法后Reducer输出就是新的toString()方法
 */

public class FlowBean implements Writable {
    private int upFlow;
    private int dFlow;
    private int totalFlow;
    private String phone;

    // java.lang.RuntimeException: java.lang.NoSuchMethodException: edu360.mr.flow.FlowBean.<init>() MUST HAVE !!
    public FlowBean(){}

    public FlowBean(String phone, int upFlow, int dFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.totalFlow = upFlow + dFlow;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public int getdFlow() {
        return dFlow;
    }

    public int getTotalFlow() {
        return totalFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public void setdFlow(int dFlow) {
        this.dFlow = dFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    /***
     * Hadoop serializable Interface
     * @param dataOutput
     * @throws IOException
     */

    // Hadoop系统在序列化该类时用的方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeUTF(phone);
        dataOutput.writeInt(dFlow);
        dataOutput.writeInt(totalFlow);


    }

    // Hadoop系统在反序列化该类时用的方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.phone = dataInput.readUTF();
        this.dFlow = dataInput.readInt();
        this.totalFlow = dataInput.readInt(); 
    }

    @Override
    public String toString(){
        return this.upFlow + "," + this.dFlow + "," + this.totalFlow;
    }
}
