package cn.edu360.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/***
 * 用于提交Mapreducer job的客户端程序
 * 功能:
 *   1.封装本次job运行时所需要的必要参数
 *   2.跟yarn进行交互,将mapreduce程序成功的启动，运行
 */
public class JobSummiterWinToYarn {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        // 在代码中设置JVM系统参数,用于给job访问HDFS的用户身份
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        // 1. 设置job运行时没要访问的默认文件系统
        conf.set("fs.dafaultFS","hdfs://hdp-01:9000");
        // 2. 设置job提交到哪里去运行
        conf.set("mapreduce.framrwork.name","yarn");
        conf.set("yarn.resourcemanager.hostname","hdp-01");
        // 3. 如果从Windows提交，需要加跨平台参数
        conf.set("mapreduce.app-submission.cross-platform","true");

        Job job = Job.getInstance(conf);

        // 1.封装参数:jar包所在位置
        job.setJarByClass(JobSummiterWinToYarn.class);

        // 2.封装参数:本次job所要调用的Mapper参数
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 3,封装参数，本次job的Mapper实现类产生结果数据的key,value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path output = new Path("/wordcount/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"),conf,"root");
        if(fs.exists(output)){
            fs.delete(output,true);
        }

        // 4.封装参数:本次job要处理的输入数据所在的路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://hdp-01:9000/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hdp-01:9000/wordcount/output"));

        // 5.封装参数: 想要启动reduce task的数量
        job.setNumReduceTasks(2);

        // 6.提交job给yarn
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:-1);


    }
}
