package edu360.mr.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.BeanUtilsBean;
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

import javax.print.attribute.standard.PresentationDirection;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoin {
    /**
     *  maptask在做数据处理时，会先调用一次setup()
     *  调完之后才对每一行反复调用map.
     *  所以处理一个文件时候可以只调用一次
     */



    public static class ReduceJoinMapper extends Mapper<LongWritable,Text,Text, JoinBean>{

        String fileName = null;
        JoinBean joinBean = new JoinBean();
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
                joinBean.set(fields[0],fields[1],"NULL",-1,"NULL","order");

            }
            // 用户表
            else{
                joinBean.set("NULL",fields[0],fields[1],Integer.parseInt(fields[2]),fields[3],"user");
            }
            k.set(joinBean.getUserId());
            context.write(k,joinBean);


        }
    }

    public static class ReduceJoinReducer extends Reducer<Text,JoinBean,JoinBean, NullWritable> {
        // 区分User表和Order表。
        // 目前问题，计算太复杂. 可能存在 user1,user2,order21,order11,order12,...order1n,user2,order21,order22,order12,...order1n  情况
        // 希望 分发时候即可实现 user1,order11,order12...user2,order21,order22, 这样reduce不需要处理逻辑，直接拿出来赋值即可.
        // 三件套上:
        //
        // 1. 现在只是比较userID,需要userID相同时比较表名. JoinBean类实现 Comparable （WritableComparable). 重载 compareTo().
        // 2. 修改分发规则 Partitioner. (Same user ID)
        // 3. Reducer 修改 GroupingComparator. 比较ID以及Order


        @Override
        protected void reduce(Text key, Iterable<JoinBean> beans, Context context) throws IOException, InterruptedException {
            ArrayList<JoinBean> orderList = new ArrayList<>();
            JoinBean userBean = null;
            try {
                // 区分两类数据
                for (JoinBean bean : beans) {
                    if ("order".equals(bean.getTableName())) {
                        // 创建新对象，把新对象拷贝到容器中。否则全部都会一样值(ArrayList)
                        JoinBean newOrderBean = new JoinBean();
                        BeanUtils.copyProperties(newOrderBean,bean);
                        orderList.add(newOrderBean);
                    }

                    else{
                        userBean = new JoinBean();
                        BeanUtils.copyProperties(userBean,bean);

                    }

                }
                // 拼接数据. 用用户组的数据补全order组
                for(JoinBean order:orderList){
                    order.setUserName(userBean.getUserName());
                    order.setUserAge(userBean.getUserAge());
                    order.setUserFriend(userBean.getUserFriend());

                    context.write(order,NullWritable.get());


                }
            }
            catch(IllegalAccessException | InvocationTargetException e){
                e.printStackTrace();
            }


        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);

            job.setJarByClass(ReduceJoin.class);

            job.setMapperClass(ReduceJoinMapper.class);
            job.setReducerClass(ReduceJoinReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(JoinBean.class);

            job.setOutputKeyClass(JoinBean.class);
            job.setOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(1);

            FileInputFormat.setInputPaths(job, new Path("D:/UoM/mapreduce24/HadoopTest/JoinInput"));
            FileOutputFormat.setOutputPath(job, new Path("D:/UoM/mapreduce24/HadoopTest/JoinOutput"));

            job.waitForCompletion(true);

        }
    }
}
