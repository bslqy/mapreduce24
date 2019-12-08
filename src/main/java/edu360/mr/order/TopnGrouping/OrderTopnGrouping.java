package edu360.mr.order.TopnGrouping;


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

public class OrderTopnGrouping {

    public static class OrderTopnMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable>{

        OrderBean orderBean = new OrderBean();
        NullWritable v = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.set(fields[0],fields[1],fields[2],Float.parseFloat(fields[3]),Integer.parseInt(fields[4]));
            context.write(orderBean,v);

        }
    }

    // 相同id的已经聚合成一起，只需输出前N条

    public static class OrderTopnReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            int topN = context.getConfiguration().getInt("order.top.n",3);
            for(NullWritable v:values){
                context.write(key,v);
                i++;
                if(i==3) return;
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.addResource("orderTopN.xml");

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderTopnGrouping.class);

        //
        job.setPartitionerClass(OrderIdPetitioner.class);
        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);


        job.setMapperClass(OrderTopnGrouping.OrderTopnMapper.class);
        job.setReducerClass(OrderTopnGrouping.OrderTopnReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(3);



//        FileInputFormat.setInputPaths(job, new Path("C:/Users/LiaoG/HadoopTest/orderTopnInput"));
//        FileOutputFormat.setOutputPath(job, new Path("C:/Users/LiaoG/HadoopTest/orderTopnOutput"));

        FileInputFormat.setInputPaths(job, new Path("D:/UoM/mapreduce24/HadoopTest/orderTopnGroupingInput"));
        FileOutputFormat.setOutputPath(job, new Path("D:/UoM/mapreduce24/HadoopTest/orderTopnGroupingOutput"));


        job.waitForCompletion(true);

    }
}
