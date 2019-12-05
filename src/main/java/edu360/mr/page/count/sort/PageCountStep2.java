package edu360.mr.page.count.sort;

import edu360.mr.flow.JobSummiter;
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

    public static class PageCountSortStep1Mapper extends Mapper<Text, Text,PageCount, NullWritable> {

    }

    public static class PageCountSortStep1Reducer extends Reducer<Text, IntWritable,Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v:values){
                count += v.get();
        }
            context.write(new Text(key),new IntWritable(count));
    }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSummiter.class);

        job.setMapperClass(PageCountSortStep1Mapper.class);
        job.setReducerClass(PageCountSortStep1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("C:/Users/LiaoG/HadoopTest/FlowCountInput"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/LiaoG/HadoopTest/FlowCountOutput"));

        job.waitForCompletion(true);
    }


}
