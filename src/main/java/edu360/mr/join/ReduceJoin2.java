package edu360.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class ReduceJoin2 {
    /**
     *  maptask在做数据处理时，会先调用一次setup()
     *  调完之后才对每一行反复调用map.
     *  所以处理一个文件时候可以只调用一次
     */



    public static class ReduceJoin2Mapper extends Mapper<LongWritable,Text,Text, JoinBean2>{

        String fileName = null;
        JoinBean2 JoinBean2 = new JoinBean2();
        Text k = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获取当前文件名
            FileSplit inputSplit = (FileSplit) context.getInputSplit();

            fileName = inputSplit.getPath().getName();

            // 可以写文件逻辑，订单文件一种处理，用户文件第二种处理
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Order 表
            if(fileName.startsWith("order")){
                JoinBean2.set(fields[0],fields[1],"NULL",-1,"NULL","order");

            }
            // 用户表
            else{
                JoinBean2.set("NULL",fields[0],fields[1],Integer.parseInt(fields[2]),fields[3],"user");
            }
            // 表名放入才能进行排序
            k.set(JoinBean2.getUserId()+","+JoinBean2.getTableName());
            context.write(k,JoinBean2);


        }
    }

    public static class ReduceJoin2Reducer extends Reducer<Text,JoinBean2,JoinBean2, NullWritable> {
        // 区分User表和Order表。
        // 目前问题，计算太复杂. 可能存在 user1,user2,order21,order11,order12,...order1n,user2,order21,order22,order12,...order1n  情况
        // 希望 分发时候即可实现 user1,order11,order12...user2,order21,order22, 这样reduce不需要处理逻辑，直接拿出来赋值即可.
        // 三件套上:
        //
        // 1. 现在只是比较userID,需要userID相同时比较表名. JoinBean2类实现 Comparable （WritableComparable). 重载 compareTo().
        // 2. 修改分发规则 Partitioner. (Same user ID)
        // 3. Reducer 修改 GroupingComparator. 比较ID以及Order


        @Override
        protected void reduce(Text key, Iterable<JoinBean2> beans, Context context) throws IOException, InterruptedException {
            // 读取第一个,必定为user


            Iterator<JoinBean2> iterator = beans.iterator();
            JoinBean2 userBean = iterator.next();
            String userName = userBean.getUserName();
            int userAge = userBean.getUserAge();
            String userFriend = userBean.getUserFriend();

            JoinBean2 order = null;
            NullWritable v = NullWritable.get();


            // @TODO 处理相同对象问题
            while(iterator.hasNext()){
                order = iterator.next();
                order.setUserName(userName);
                order.setUserAge(userAge);
                order.setUserFriend(userFriend);
                context.write(order,v);
            }



        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);

            job.setJarByClass(ReduceJoin2.class);

            job.setPartitionerClass(ReduceJoin2Partitioner.class);
            job.setSortComparatorClass(ReduceJoin2SortingComparator.class);
            job.setGroupingComparatorClass(ReduceJoin2GroupingComparator.class);

            job.setMapperClass(ReduceJoin2Mapper.class);
            job.setReducerClass(ReduceJoin2Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(JoinBean2.class);

            job.setOutputKeyClass(JoinBean2.class);
            job.setOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(5);

            FileInputFormat.setInputPaths(job, new Path("D:/UoM/mapreduce24/HadoopTest/Join2Input"));
            FileOutputFormat.setOutputPath(job, new Path("D:/UoM/mapreduce24/HadoopTest/Join2Output"));

            job.waitForCompletion(true);

        }
    }
}
