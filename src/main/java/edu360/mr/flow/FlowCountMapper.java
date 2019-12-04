package edu360.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/***
 *
 *  统计总流量
 *
 *  聚合条件: 用户名
 *  输出: 流量总量,上行,下行
 *
 */

public class FlowCountMapper  extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phone = fields[1];
        int upFlow = Integer.parseInt(fields[fields.length - 3]);
        int dFlow = Integer.parseInt(fields[fields.length-2]);

        // Send data
        context.write(new Text (phone),new FlowBean(phone,upFlow,dFlow));

    }
}
