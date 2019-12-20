package cn.edu360.app.log.mr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AppLogDataClean {

    public static class AppLogDataCleanMapper extends Mapper<LongWritable,Text, Text, NullWritable>{
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
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.DEVICE_ID)) ){
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

            if ("andriod".equals(headerObj.getString(GlobalConstants.OS_NAME).trim()) ){
                user_id = StringUtils.isNotBlank(headerObj.getString(GlobalConstants.ANDRIOD_ID))?headerObj.getString(GlobalConstants.ANDRIOD_ID):headerObj.getString("device_id");
            } else {
                user_id = headerObj.getString(GlobalConstants.ANDRIOD_ID);
            }




        }
    }

}
