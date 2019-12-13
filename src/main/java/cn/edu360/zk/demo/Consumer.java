package cn.edu360.zk.demo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Consumer {
    ZooKeeper zk = null;

    public void connectZK() throws IOException {
        zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
    public static void main(String[] args) throws IOException {
        // Initialize Zookeeper



        // Check the online ip

        // 向服务器发送时间查询请求
    }
}
