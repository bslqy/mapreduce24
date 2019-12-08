package edu360.mr.order.TopnGrouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPetitioner extends Partitioner<OrderBean, NullWritable> {



    @Override
    public int getPartition(OrderBean key, NullWritable value, int i) {
        return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % i;
    }
}
