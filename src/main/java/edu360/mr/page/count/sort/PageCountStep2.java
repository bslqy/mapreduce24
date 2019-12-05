package edu360.mr.page.count.sort;

import edu360.mr.flow.JobSubmitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageCountStep2 {

    public static class PageCountSortStep2Mapper extends Mapper<LongWritable, Text,PageCount, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");

            PageCount pageCount = new PageCount();
            pageCount.set(split[0],Integer.parseInt(split[1]));
            context.write(pageCount,NullWritable.get());
        }
    }


    public static class PageCountSortStep2Reducer extends Reducer<PageCount, NullWritable,PageCount, NullWritable> {
        @Override
        protected void reduce(PageCount key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountStep2.class);

        job.setMapperClass(PageCountSortStep2Mapper.class);
        job.setReducerClass(PageCountSortStep2Reducer.class);

        job.setMapOutputKeyClass(PageCount.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(PageCount.class);
        job.setOutputValueClass(NullWritable.class);

        // Must be 1
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("C:/Users/LiaoG/HadoopTest/PageCountSortOutput/Step1"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/LiaoG/HadoopTest/PageCountSortOutput/Step2"));

        job.waitForCompletion(true);
    }


}
