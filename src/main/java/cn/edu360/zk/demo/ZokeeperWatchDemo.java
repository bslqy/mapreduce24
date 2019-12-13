package cn.edu360.zk.demo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;

import java.awt.*;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZokeeperWatchDemo {

    ZooKeeper zk = null;
    @Before
    public void init() throws Exception {

        zk =  new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181",2000,new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected && watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                    // 只监听一次
                    System.out.println(watchedEvent.getPath());
                    System.out.println(watchedEvent.getType());

                    // Handling
                    System.out.println("change");

                    //实现长期监听,需要在zookeeper构造器中写出回调逻辑.再次注册相同路径
                    try {
                        zk.getData("/mygirl", true, null);
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }


                }
                else if (watchedEvent.getState() == Event.KeeperState.SyncConnected && watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    System.out.println("Child node change");
                }
            }
        });
    }

    public void testGetWatach() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] data = zk.getData("/mygirls/",true,null);

        //监听节点的子节点
        List<String> children = zk.getChildren("/mygirls", true, null);

        System.out.println(new String(data,"UTF-8"));

        Thread.sleep(Long.MAX_VALUE);
    }


}
