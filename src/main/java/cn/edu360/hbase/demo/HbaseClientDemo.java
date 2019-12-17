package cn.edu360.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class HbaseClientDemo {
    Connection conn = null;


    //构造一个连接对象
    @Before
    public void getConn() throws IOException {

        Configuration conf = HBaseConfiguration.create(); // 会自动加载hbase-site.xml
        conf.set("hbase.zookeeper.quorum","hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181"); //如不告知则无法连接
        conn = ConnectionFactory.createConnection(conf);

    }


    /**
     * DDL
     */
    @Test
    public void testCreateTable() throws IOException {


        // 从连接中构造一个DDL操作器
        Admin admin = conn.getAdmin();

        // 创建一个表定义描述对象
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));

        // 创建列族定义描述对象
        HColumnDescriptor hColumnDescriptor_1 = new HColumnDescriptor("base_info");
        hColumnDescriptor_1.setMaxVersions(3);

        HColumnDescriptor hColumnDescriptor_2 = new HColumnDescriptor("extra_info");

        // 将列族定义对象加入表定义对象中
        hTableDescriptor.addFamily(hColumnDescriptor_1);
        hTableDescriptor.addFamily(hColumnDescriptor_2);

        // 用ddl操作器对象: admin来建表
        admin.createTable(hTableDescriptor);

        admin.close();
        conn.close();

    }

    /**
     * 删除表
     */
    @Test
    public void TestDropTable() throws IOException {
        Admin admin = conn.getAdmin();

        //停用表
        admin.disableTable(TableName.valueOf("user_info"));
        // 删除表
        admin.deleteTable(TableName.valueOf("user_info"));
    }

    // 修改表定义
    @Test
    public void testAlterTable() throws IOException {
        Admin admin = conn.getAdmin();

        // 取出旧的表定义信息
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));

        // 新构造一个列族定义
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL); // 布隆过滤器用来判断一行数据是否存在过.

        // 将列族定义添加到表定义对象中
        tableDescriptor.addFamily(hColumnDescriptor);

        // 将修改过的表定义交个admin去提交
        admin.modifyTable(TableName.valueOf("user_info"), tableDescriptor);

        admin.close();
        conn.close();

    }





    /***
     * DML
     */
}
