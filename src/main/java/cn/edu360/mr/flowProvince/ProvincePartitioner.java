package cn.edu360.mr.flowProvince;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/***
 * MapTask通过这个类的getPartition方法，来计算它所产生的每一对kv数据该分发给哪一个reduce task
 */

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    static HashMap<String,Integer> codeMap = new HashMap<>();

    static{
        codeMap.put("135",0);
        codeMap.put("136",0);
        codeMap.put("137",0);
        codeMap.put("138",0);
        codeMap.put("139",0);


    }

    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
        Integer code = codeMap.get(key.toString().substring(0,3));
        return code==null?5:code;
    }
}
