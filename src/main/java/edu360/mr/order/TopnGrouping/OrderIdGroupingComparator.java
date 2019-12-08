package edu360.mr.order.TopnGrouping;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupingComparator extends WritableComparator {


    // 必须在构造器中写入类型
    public OrderIdGroupingComparator() {
        super(OrderBean.class,true);
    }

    //反序列化后对比，必须强转
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;


        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
