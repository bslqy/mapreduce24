package cn.edu360.mr.order.TopnGrouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {


    // 按照orderId分发数据
    @Override
    public int getPartition(OrderBean key, NullWritable value, int i) {
        return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % i;
    }
}
