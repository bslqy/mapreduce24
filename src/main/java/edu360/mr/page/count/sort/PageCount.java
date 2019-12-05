package edu360.mr.page.count.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/***
 * Key要发出去，必须序列化。
 * 并且Key本身有排序机制。所以必须实现2个接口
 */
public class PageCount implements WritableComparable<PageCount> {
    private String page;
    private int count;

    public void set(String page,int count){
        this.page = page;
        this.count = count;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(PageCount o) {
        return o.getCount() - this.getCount() == 0?this.page.compareTo(o.getPage()):o.getCount()-this.count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.page);
        out.writeInt(this.count);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.page = in.readUTF();
        this.count = in.readInt();

    }

    @Override
    public String toString() {
        return this.page+","+this.count;
    }
}
