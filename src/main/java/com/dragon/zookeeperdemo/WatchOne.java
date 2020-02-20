package com.dragon.zookeeperdemo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @author：Dragon Wen
 * @email：18475536452@163.com
 * @date：Created in 2020/2/20 11:14
 * @description：一次性通知机制
 * @modified By：
 * @version: $
 */
public class WatchOne {

    private static final String CONNECTSTRING = "192.168.163.129:2181";
    private static final String PATH = "/dragon";
    private static final int SESSION_TIMEOUT=20*1000;

    //实例变量
    private ZooKeeper zooKeeper;

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

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
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createZNode(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 获得当前节点的最新值
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getZNode(String path) throws KeeperException, InterruptedException {
        String result = "";
        byte[] data = zooKeeper.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    triggerValue(path);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());
        result = new String(data);
        return result;
    }

    //triggerValue
    private void triggerValue(String path) throws KeeperException, InterruptedException {
        String result = "";
        byte[] data = zooKeeper.getData(path, false, new Stat());
        result = new String(data);
        System.out.println("triggerValue="+result);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        WatchOne watchOne = new WatchOne();
        watchOne.setZooKeeper(watchOne.startZK());
        if(watchOne.getZooKeeper().exists(PATH,false) == null){
            watchOne.createZNode(PATH, "AAA");
            String zNode = watchOne.getZNode(PATH);
            System.out.println("zNode="+zNode);
        } else {
            System.out.println("this zNode is created");
        }

        Thread.sleep(Long.MAX_VALUE);
    }
}
