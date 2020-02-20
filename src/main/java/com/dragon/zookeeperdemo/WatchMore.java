package com.dragon.zookeeperdemo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @author：Dragon Wen
 * @email：18475536452@163.com
 * @date：Created in 2020/2/20 11:54
 * @description：多次性通知机制
 * @modified By：
 * @version: $
 */
public class WatchMore {

    private static final String CONNECTSTRING = "192.168.163.129:2181";
    private static final String PATH = "/dragon";
    private static final int SESSION_TIMEOUT=20*1000;

    //实例变量
    private ZooKeeper zooKeeper = null;
    private String oldValue = null;
    private String newValue = null;

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public void setNewValue(String newValue) {
        this.newValue = newValue;
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
        byte[] byteArray = zooKeeper.getData(path, new Watcher() {
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
        result = new String(byteArray);
        oldValue = result;
        return result;
    }

    //triggerValue
    private boolean triggerValue(String path) throws KeeperException, InterruptedException {
        String result = "";
        byte[] byteArray = zooKeeper.getData(path,new Watcher() {
            @Override
            public void process(WatchedEvent event)
            {
                try
                {
                    triggerValue(path);
                }catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());

         result = new String(byteArray);
         newValue = result;

        if(oldValue.equals(newValue))
        {
            System.out.println("there is no change --------");
            return false;
        }else{
            System.out.println("oldValue: "+oldValue+"\t"+"newValue: "+newValue);
            oldValue = newValue;
            return true;
        }

    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        WatchMore watchMore = new WatchMore();
        watchMore.setZooKeeper(watchMore.startZK());
        if(watchMore.getZooKeeper().exists(PATH,false) == null){
            watchMore.createZNode(PATH, "AAA");
            String zNode = watchMore.getZNode(PATH);
            System.out.println("main(String[] args) ------ init String result="+zNode);
        } else {
            System.out.println("this zNode has already exists");
        }

        Thread.sleep(Long.MAX_VALUE);
    }
}
