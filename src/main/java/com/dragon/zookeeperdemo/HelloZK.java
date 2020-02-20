package com.dragon.zookeeperdemo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @author：Dragon Wen
 * @email：18475536452@163.com
 * @date：Created in 2020/2/19 22:56
 * @description：此处为Client端，CentOS为zooKeeper的Server端
 * @modified By：
 * @version: $
 */
public class HelloZK {

    private static final String CONNECTSTRING = "192.168.163.129:2181";
    private static final String PATH = "/dragon";
    private static final int SESSION_TIMEOUT=20*1000;

    /**
     * 通过JAVA程序，新建连接zk
     * @return
     * @throws IOException
     */
    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    /**
     * 新建一个znode节点
     * @param zooKeeper
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createZNode(ZooKeeper zooKeeper, String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 获得当前节点的最新值
     * @param zooKeeper
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getZNode(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
        String result = "";
        byte[] data = zooKeeper.getData(path, false, new Stat());
        result = new String(data);
        return result;
    }

    /**
     * 关闭连接
     * @param zooKeeper
     * @throws InterruptedException
     */
    public void stopZK(ZooKeeper zooKeeper) throws InterruptedException {
        if(zooKeeper != null){
            zooKeeper.close();
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        HelloZK helloZK = new HelloZK();
        ZooKeeper zooKeeper = helloZK.startZK();
        if(zooKeeper.exists(PATH, false) == null){
            helloZK.createZNode(zooKeeper, PATH, "您好，ZooKeeper");
            String zNode = helloZK.getZNode(zooKeeper, PATH);
            System.out.println("zNode="+zNode);
        } else {
            System.out.println("this zNode is created");
        }
        helloZK.stopZK(zooKeeper);
    }
}
