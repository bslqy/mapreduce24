package cn.edu360.mr.flowProvince;

import cn.edu360.mr.flow.FlowBean;
import cn.edu360.mr.flow.FlowCountMapper;
import cn.edu360.mr.flow.FlowCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class JobSubmitter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        // Which class used for parition
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(6);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(cn.edu360.mr.flow.FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);



        FileInputFormat.setInputPaths(job, new Path("C:/Users/LiaoG/HadoopTest/FlowCountInput"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/LiaoG/HadoopTest/FlowCountOutput"));

        job.waitForCompletion(true);

    }
}
