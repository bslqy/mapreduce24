package cn.edu360.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/***
 * KEYIN: 是map task读取到的一行数据的key的类型，是一行起始偏移量long
 * VALUEIN: map task读取到的数据value 的类型，是一行的内容String
 *
 * KEYOUT: 用户自定义方法结果kv数据的key类型
 * VALUEOUT: 用户自定义方法结果kv数据的value类型
 */

public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 切单词
        String line = value.toString();
        String[] words = line.split(" ");
        for(String word:words)
        {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
