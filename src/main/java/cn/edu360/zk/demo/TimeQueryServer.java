package cn.edu360.zk.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class TimeQueryServer {

    ZooKeeper zk = null;

    public void connectZK() throws IOException {
        zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {


            }
        });
    }

    /**
     * 判断注册节点的父节点存不存在，如果不存在，则创建
     * @param hostname
     * @param port
     * @throws KeeperException
     * @throws InterruptedException
     */

    public void registerServerInfo(String hostname,String port) throws KeeperException, InterruptedException {

        Stat stat = zk.exists("/servers",false);
        if(stat == null) {
            zk.create("/servers", (hostname + ":" + port).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 注册服务器数据到zk的约定注册节点下
        String create = zk.create("/servers/server", (hostname + ":" + port).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " 向zk注册信息成功，注册节点为: /servers" + create);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // Initialize zk client
        TimeQueryServer timeQueryServer  = new TimeQueryServer();

        // Register server
        timeQueryServer.connectZK();

        timeQueryServer.registerServerInfo(args[0],args[1]);

        // Handling
        new TimeQueryService(Integer.parseInt(args[1])).start();


    }
}
