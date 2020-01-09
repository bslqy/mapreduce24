package cn.edu360.mr.order.TopN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

import java.util.Collections;

/***
 * 生成每个用户采购中最高的N个订单
 */

public class OrderTopn {
    public static class OrderTopnMapper extends Mapper<LongWritable, Text,Text,OrderBean>{
        OrderBean orderBean = new OrderBean();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.set(fields[0],fields[1],fields[2],Float.parseFloat(fields[3]),Integer.parseInt(fields[4]));
            k.set(fields[0]);


            // 从这里交给maptask的kv对象，会被maptask序列化后存储，所以不需要担心覆盖问题
            context.write(k,orderBean);
        }
    }

    public static class OrderTopnReducer extends Reducer<Text,OrderBean,OrderBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
            // 获取TopN的参数
            int topN = context.getConfiguration().getInt("order.top.n",3);
            ArrayList<OrderBean> beanList = new ArrayList<>();

            // reduce task 提供的value迭代器，每次迭代返回给我们的都是同一个对象,必须创建新对象
            for (OrderBean orderBean:values){
                OrderBean newBean = new OrderBean();
                newBean.set(orderBean.getOrderId(),orderBean.getUserId(),orderBean.getPdtName(),orderBean.getPrice(),orderBean.getNumber());
                beanList.add(newBean);
            }
            // 对beanlist中的orderBean进行排序(按照总金额的大小倒序)

            Collections.sort(beanList);
            for(int i = 0; i<topN;i++ ){
                context.write(beanList.get(i),NullWritable.get());

            }


        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.addResource("orderTopN.xml");

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderTopn.class);


        job.setMapperClass(OrderTopnMapper.class);
        job.setReducerClass(OrderTopnReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(3);

//        FileInputFormat.setInputPaths(job, new Path("C:/Users/LiaoG/HadoopTest/orderTopnInput"));
//        FileOutputFormat.setOutputPath(job, new Path("C:/Users/LiaoG/HadoopTest/orderTopnOutput"));

        FileInputFormat.setInputPaths(job, new Path("D:/UoM/mapreduce24/HadoopTest/orderTopnInput"));
        FileOutputFormat.setOutputPath(job, new Path("D:/UoM/mapreduce24/HadoopTest/orderTopnOutput"));


        job.waitForCompletion(true);

    }
}
