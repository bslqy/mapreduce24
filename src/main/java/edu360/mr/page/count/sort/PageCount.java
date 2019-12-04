package edu360.mr.page.count.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PageCount implements WritableComparable<PageCount> {
    private String page;
    private int count;

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
    public int compareTo(PageCount pageCount) {
        return pageCount.getCount() - this.getCount() == 0?this.page.compareTo(pageCount.getPage()):pageCount.getCount()-this.count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
