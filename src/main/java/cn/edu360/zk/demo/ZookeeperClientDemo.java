package cn.edu360.zk.demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZookeeperClientDemo {

    ZooKeeper zk = null;
    @Before
    public void init() throws Exception {
        zk =  new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181",2000,null);
    }

    @Test
    public void testCreate() throws IOException, KeeperException, InterruptedException {
        // watcher is event handler

       String create = zk.create("/eclipse","hello eclipse".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println(create);

        zk.close();
    }

    @Test
    public void testUpdate() throws UnsupportedEncodingException, KeeperException, InterruptedException {
        zk.setData("/eclipse","胡旸旸".getBytes("UTF-8"),-1); // -1 all version
        zk.close();
    }

    @Test
    public void get() throws KeeperException, InterruptedException, UnsupportedEncodingException {

        byte[] data = zk.getData("/eclipse", false, null); // stat:版本. null表示最新版本
        System.out.println(new String(data,"UTF-8"));
    }

    @Test
    public void ListChildren() throws  Exception{
        List<String> children = zk.getChildren("/cc",false);

        // 不带全路径
        byte[] data = zk.getData("/eclipse",false,null);
        for(String child:children){
            System.out.println(child);
        }

        zk.close();

    }

    @Test
    public void testRm() throws KeeperException, InterruptedException {
        zk.delete("/eclipse",-1);
        zk.close();
    }

}
