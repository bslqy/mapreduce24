package cn.edu360.app.log.mr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class AppLogDataClean {



    public static class AppLogDataCleanMapper extends Mapper<LongWritable,Text, Text, NullWritable>{
        // 实例化对象
        MultipleOutputs<Text,NullWritable> mos = null;
        Text k = null;
        NullWritable v = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k  = new Text();
            mos = new MultipleOutputs<Text, NullWritable>(context); // 多路输出器
            v = NullWritable.get();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            JSONObject jsonObject = JSON.parseObject(value.toString());
            JSONObject headerObj = jsonObject.getJSONObject(GlobalConstants.HEADER);



            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.SDK_VER)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.TIME_ZONE)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.COMMIT_ID)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.COMMIT_TIME)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.PID)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.APP_TOKEN)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.APP_ID)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.DEVICE_ID))  || headerObj.getString(GlobalConstants.DEVICE_ID).length() < 17){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.DEVICE_ID_TYPE)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.RELEASE_CHANNEL)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.APP_VER_NAME)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.APP_VER_CODE)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.OS_NAME)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.OS_VER)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.LANGUAGE)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.COUNTRY)) ){
                return;
            }            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.MANUFACTURE)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.DEVICE_MODEL)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.RESOLUTION)) ){
                return;
            }
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.NET_TYPE)) ){
                return;
            }

            /***
             *  Create id from os_name
             */
            String user_id = "";
            if ("android".equals(headerObj.getString(GlobalConstants.OS_NAME).trim()) ){
                user_id = StringUtils.isNotBlank(headerObj.getString(GlobalConstants.ANDRIOD_ID))?headerObj.getString(GlobalConstants.ANDRIOD_ID):headerObj.getString(GlobalConstants.DEVICE_ID);
            } else {
                user_id = headerObj.getString(GlobalConstants.ANDRIOD_ID);
            }

            /***
             * 输出结果
             */
            //多类输出. 如果object是一个Android，生成一个自己的路径. Mac和Iphone也是



            headerObj.put(GlobalConstants.USER_ID,user_id);
            k.set(JsonToStringUtil.toString(headerObj));

            if("android".equals(headerObj.getString(GlobalConstants.OS_NAME))){
                mos.write(k,v,"android/android");
            }else {
                mos.write(k,v,"ios/ios");
            }
            // 需要关闭mos.在最后关闭.所以要写一个cleanUp方法

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance();

        job.setJarByClass(AppLogDataClean.class);

        job.setMapperClass(AppLogDataCleanMapper.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(NullWritable.class);

        //避免根目录下的冗余输出,只在分区文件夹内输出
        //避免生成part-m-00000等文件,因为数据已经交给MultipleOutputs输出
        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0: 1);

    }

}
