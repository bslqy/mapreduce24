package edu360.mr.page.topN;

public class PageCount implements Comparable<PageCount>{

    private String page;
    private int count;

    public void set (String page,int count){
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

    // If two are the same, compare the page. Otherwise, compare the value.
    @Override
    public int compareTo(PageCount o) {
        return o.getCount() - this.count == 0? this.page.compareTo(o.getPage()):o.getCount() - this.getCount();
    }
}
