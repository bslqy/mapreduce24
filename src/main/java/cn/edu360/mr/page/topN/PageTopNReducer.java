package cn.edu360.mr.page.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PageTopNReducer extends Reducer<Text, IntWritable ,Text, IntWritable> {
    // 把运算结果放入TreeMap。在Cleanup中输出前五条

    TreeMap<PageCount,Object> treeMap = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v: values){
            count += v.get();
        }

        PageCount pageCount = new PageCount();
        pageCount.set(key.toString(),count);
        
        treeMap.put(pageCount,null);


//        context.write(new Text(key),new IntWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Map.Entry<PageCount, Object>> entrySet = treeMap.entrySet();

        // How to pass parameter? Use configuration.
        // Where is configuration created? jobSubmitter
        Configuration conf =  context.getConfiguration();
        int topN = conf.getInt("top.n",5);

        int i = 0;

        for (Map.Entry<PageCount,Object> entry: entrySet){
            context.write(new Text(entry.getKey().getPage()),new IntWritable(entry.getKey().getCount()));
            i++;
            if (i==topN){
                return;
            }
        }


    }
}
