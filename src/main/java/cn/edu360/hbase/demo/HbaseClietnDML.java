package cn.edu360.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class HbaseClietnDML {

    Connection conn = null;

    //构造一个连接对象
    @Before
    public void getConn() throws IOException {

        Configuration conf = HBaseConfiguration.create(); // 会自动加载hbase-site.xml
        conf.set("hbase.zookeeper.quorum","hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181"); //如不告知则无法连接
        conn = ConnectionFactory.createConnection(conf);

    }

    @Test
    public void testPut() throws Exception{
       //
        Table  table = conn.getTable(TableName.valueOf("user_info"));

        //
        Put put = new Put(Bytes.toBytes(1));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"),Bytes.toBytes("张三"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes("18"));
        put.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("addr"),Bytes.toBytes("北京"));

        //
        table.put(put);

        table.close();
        conn.close();

    }


    @Test
    public void testManyPuts() throws Exception{
        Table table = conn.getTable(TableName.valueOf("user_info"));
        ArrayList<Put> puts = new ArrayList<>();

        for (int i = 0;i<1000;i++){

            Put put = new Put(Bytes.toBytes("" + i));
            put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"),Bytes.toBytes("张三"+i));
            put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes((18+i)+""));
            put.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("addr"),Bytes.toBytes("北京"));

            puts.add(put);
        }

        table.put(puts);
    }



    @Test
    public void testDelete() throws Exception{
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Delete delete1 = new Delete(Bytes.toBytes("001"));
        Delete delete2 = new Delete(Bytes.toBytes("002"));

        delete2.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("addr"));

        ArrayList<Delete> dels = new ArrayList<>();

        dels.add(delete1);
        dels.add(delete2);

        table.delete(dels);
        table.close();
        conn.close();
        
    }

    @Test
    public void testGet() throws Exception{
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Get get = new Get("002".getBytes());

        Result result = table.get(get);

        //从用户结果中指定某个key的value  取一个值
        result.getValue("base_info".getBytes(),"age".getBytes());

        // 遍历所有值
        CellScanner cellScanner = result.cellScanner();
        while(cellScanner.advance()){
            Cell cell = cellScanner.current();

            byte[] rowArray = cell.getRowArray();// current kv
            byte[] familyArray = cell.getFamilyArray();  // 列族名
            byte[] qualiferArray = cell.getQualifierArray(); //列名
            byte[] valueArray = cell.getValueArray(); // value名

            System.out.println("KV: " + new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
            System.out.println("Family Name: " + new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
            System.out.println("Column Name: " + new String(qualiferArray,cell.getQualifierOffset(),cell.getQualifierLength()));
            System.out.println("Value : " + new String(valueArray,cell.getValueOffset(),cell.getValueLength()));


        }

    }

    /***
     * 按行键范围进行查询
     * @throws Exception
     */
    @Test
    public void testScan() throws Exception{
        Table table = conn.getTable(TableName.valueOf("user_info"));

        // 包含起始，不包含结束
        Scan scan = new Scan("001".getBytes(), "005".getBytes());

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()){

            Result result = iterator.next();

            Cell cell = result.current();

            byte[] rowArray = cell.getRowArray();// current kv
            byte[] familyArray = cell.getFamilyArray();  // 列族名
            byte[] qualiferArray = cell.getQualifierArray(); //列名
            byte[] valueArray = cell.getValueArray(); // value名

            System.out.println("KV: " + new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
            System.out.println("Family Name: " + new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
            System.out.println("Column Name: " + new String(qualiferArray,cell.getQualifierOffset(),cell.getQualifierLength()));
            System.out.println("Value : " + new String(valueArray,cell.getValueOffset(),cell.getValueLength()));


        }

        System.out.println("---------------------------------");

    }
}

