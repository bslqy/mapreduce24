package cn.edu360.zk.demo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Consumer {

    // 定义一个list用于存放最新的在线服务列表
    private volatile ArrayList<String> onlineServers = new ArrayList<>();



    // 构造zk连对象
    ZooKeeper zk = null;

    public void connectZK() throws IOException {
        zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {
            // 事件回调逻辑
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected && watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try {
                        // 事件回调逻辑中在此查询zk上的在线服务器节点即可.查询逻辑中又注册了子节点变化的事件监听.
                        getOnlineServers();
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }
        });
    }

    public void sendRequest() throws InterruptedException, IOException {
        // 挑选一台当前在线的服务器

        Random random = new Random();
        while(true) {
            try {
                int nextInt = random.nextInt(onlineServers.size());
                String server = onlineServers.get(nextInt);

                String hostname = server.split(":")[0];
                int port = Integer.parseInt(server.split(":")[1]);

                System.out.println("本次请求挑选的服务器为:" + port);

                Socket socket = new Socket(hostname, port);
                OutputStream outputStream = socket.getOutputStream();
                InputStream inputStream = socket.getInputStream();

                outputStream.write("haha".getBytes());
                outputStream.flush();

                byte[] buff = new byte[256];
                int read = inputStream.read(buff);
                System.out.println("服务器响应的时间为:" + new String(buff, 0, read));

                outputStream.close();
                inputStream.close();
                socket.close();

                Thread.sleep(5000);

            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    // 查询在线服务器
    public void getOnlineServers() throws KeeperException, InterruptedException {
        // 注册监听，持续监听
        List<String> children = zk.getChildren("/servers", true);
        ArrayList<String> servers = new ArrayList<>();
        for(String child: children){
            byte[] data = zk.getData("/servers" + child, false, null);

            String serverInfo = new String(data);
            servers.add(serverInfo);
        }
        onlineServers = servers;
        System.out.println("查询了一次在zk,当前在线的服务器有:"+servers);
    }
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // Initialize Zookeeper
        Consumer consumer = new Consumer();
        consumer.connectZK();

        // Check the online ip
        consumer.getOnlineServers();


        // 向服务器发送时间查询请求
        consumer.sendRequest();
    }
}
