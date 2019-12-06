package edu360.mr.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;

import java.io.IOException;

public class IndexStep2 {

    public static class IndexStepTwoMapper extends Mapper<LongWritable,Text,Text,Text> {

        // input: <word - file, count>
        // output: <word, new Text(file1-> count,file2 -> count...)>
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] splits = line.split("-");
            String output = splits[1].replaceAll("\t","-->");

            context.write(new Text(splits[0]),new Text(output));

        }

    }

    public static class IndexStepTwoReducer extends Reducer<Text,Text,Text,Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text v:values){
                sb.append(v.toString()).append("\t");
            }
            context.write(key,new Text(sb.toString().substring(0,sb.length()-1)));
        }
    }




    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStep2.class);


        job.setMapperClass(IndexStep2.IndexStepTwoMapper.class);
        job.setReducerClass(IndexStep2.IndexStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("C:/Users/LiaoG/HadoopTest/indexCountOutput/Step1"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/LiaoG/HadoopTest/indexCountOutput/Step2"));

        job.waitForCompletion(true);

    }
}
