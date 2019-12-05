package edu360.mr.page.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Properties;

/***
 *  核心思想
 *  1. 在Reducer类的cleanup方法里进行选择性输出.
 *  2. 传入参数控制Reducer中输出的前几.
 */

public class jobSubmitter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // Through xx-oo.xml
        Configuration conf = new Configuration();

        // Automatically load
        conf.addResource("xx-oo.xml");

        /***
         *
        // Through Direct Input
        conf.setInt("top.n",3);

        //Through CMD argument
        conf.setInt("top.n",Integer.parseInt(args[0]));

        // Through Properties
        Properties pro = new Properties();
        pro.load(jobSubmitter.class.getClassLoader().getResourceAsStream("topn.properties"));
        conf.setInt("top.n",Integer.parseInt(pro.getProperty("top.n")));

         ***/

        Job job = Job.getInstance(conf);

        job.setMapperClass(PageTopNMapper.class);
        job.setReducerClass(PageTopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job, new Path("C:/Users/LiaoG/HadoopTest/TopNCountInput"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/LiaoG/HadoopTest/TopNCountOutput"));

        job.waitForCompletion(true);


    }
}
