package edu360.mr.join;

import edu360.mr.order.TopnGrouping.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ReduceJoin2Partitioner extends Partitioner<Text, JoinBean2> {

    // 按照ID分发
    @Override
    public int getPartition(Text key, JoinBean2 value, int i) {

        return (key.toString().split(",")[0].hashCode() & Integer.MAX_VALUE) % i;
    }
}