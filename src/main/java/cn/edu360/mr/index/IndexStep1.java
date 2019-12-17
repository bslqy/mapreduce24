package cn.edu360.mr.index;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 需求： 统计每个单词出现的总次数以及出现的在哪几个文件中
 *
 */

public class IndexStep1  {

    // 算出某个单词在某个文件内有几个
    public static class IndexStepOneMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

        // <word+'\t'+'filename'>  作为key发出去

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();

            String[] words = value.toString().split(" ");
            for(String w: words){
                context.write(new Text(w + "-" + fileName),new IntWritable(1));
            }


        }
    }

    public static class IndexStepOneReducer extends Reducer <Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value: values){
                count += value.get();
            }
            context.write(key,new IntWritable(count));
        }


    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStep1.class);


        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path("C:/Users/LiaoG/HadoopTest/indexCountInput"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/LiaoG/HadoopTest/indexCountOutput/Step1"));

        job.waitForCompletion(true);

    }



}