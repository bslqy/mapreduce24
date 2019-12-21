package cn.edu360.app.log.mr;

import com.alibaba.fastjson.JSONObject;

public class JsonToStringUtil  {
    public static String toString(JSONObject jsonObject){
        StringBuilder sb = new StringBuilder();

        sb.append(jsonObject.get(GlobalConstants.SDK_VER)).append("\001")
                .append(jsonObject.get(GlobalConstants.TIME_ZONE)).append("\001")
                .append(jsonObject.get(GlobalConstants.COMMIT_ID)).append("\001")
                .append(jsonObject.get(GlobalConstants.COMMIT_TIME)).append("\001")
                .append(jsonObject.get(GlobalConstants.PID)).append("\001")
                .append(jsonObject.get(GlobalConstants.APP_TOKEN)).append("\001")
                .append(jsonObject.get(GlobalConstants.APP_ID)).append("\001")
                .append(jsonObject.get(GlobalConstants.DEVICE_ID)).append("\001")
                .append(jsonObject.get(GlobalConstants.DEVICE_ID_TYPE)).append("\001")
                .append(jsonObject.get(GlobalConstants.RELEASE_CHANNEL)).append("\001")
                .append(jsonObject.get(GlobalConstants.APP_VER_NAME)).append("\001")
                .append(jsonObject.get(GlobalConstants.APP_VER_CODE)).append("\001")
                .append(jsonObject.get(GlobalConstants.OS_NAME)).append("\001")
                .append(jsonObject.get(GlobalConstants.OS_VER)).append("\001")
                .append(jsonObject.get(GlobalConstants.LANGUAGE)).append("\001")
                .append(jsonObject.get(GlobalConstants.COUNTRY)).append("\001")
                .append(jsonObject.get(GlobalConstants.MANUFACTURE)).append("\001")
                .append(jsonObject.get(GlobalConstants.DEVICE_MODEL)).append("\001")
                .append(jsonObject.get(GlobalConstants.RESOLUTION)).append("\001")
                .append(jsonObject.get(GlobalConstants.NET_TYPE	)).append("\001")
                .append(jsonObject.get(GlobalConstants.ACCOUNT	)).append("\001")
                .append(jsonObject.get(GlobalConstants.APP_DEVICE_ID)).append("\001")
                .append(jsonObject.get(GlobalConstants.MAC)).append("\001")
                .append(jsonObject.get(GlobalConstants.ANDRIOD_ID)).append("\001")
                .append(jsonObject.get(GlobalConstants.IMEI)).append("\001")
                .append(jsonObject.get(GlobalConstants.CID_SN)).append("\001")
                .append(jsonObject.get(GlobalConstants.BUILD_NUM)).append("\001")
                .append(jsonObject.get(GlobalConstants.MOBILE_DATA_TYPE)).append("\001")
                .append(jsonObject.get(GlobalConstants.PROMOTION_CHANNEL)).append("\001")
                .append(jsonObject.get(GlobalConstants.CARRIER)).append("\001")
                .append(jsonObject.get(GlobalConstants.CITY)).append("\001")
                .append(jsonObject.get(GlobalConstants.USER_ID)).append("\001");

        System.out.println(sb.toString());

            return sb.toString();

    }
}
