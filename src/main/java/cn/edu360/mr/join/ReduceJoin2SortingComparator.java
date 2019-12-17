package cn.edu360.mr.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReduceJoin2SortingComparator extends WritableComparator {

    public ReduceJoin2SortingComparator() {
        super(Text.class,true);
    }

    // 按照ID分组
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text o1 = (Text) a;
        Text o2 = (Text) b;

        String userId1 = o1.toString().split(",")[0];
        String userId2 = o1.toString().split(",")[0];

        if (!userId1.equals(userId2)){
            return userId1.compareTo(userId2);
        }
        else return -1*(o1.toString().split(",")[1].compareTo(o2.toString().split(",")[1]));

        }
}
