package com.aura.bigdata;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZooKeeperTest {
    public static void main(String[] args) throws Exception {
        //zookeeper的api入口类，zookeeper
        String zkStr = "bigdata01:2181,bigdata02:2181,bigdata03:2181";
        ZooKeeper zk = new ZooKeeper(zkStr, 3000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //当关联目录发生变化的时候，变会调用该方法
                System.out.println("-------process-------");
            }
        });

        String path = "/test";
        if(zk.exists(path, false) == null) {
            System.out.println("/test不存在，创建它");
            String createdPath = zk.create(path, "192.168.63.60".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            if(createdPath != null) {
                System.out.println(createdPath + ", 创建成功");
            } else {
                System.out.println(path + "创建失败");
            }
        }
        //指定zk节点对应的数据
        Stat stat = zk.setData(path, "oldli".getBytes(), -1);
        System.out.println("dataLen: " + stat.getDataLength());

    }
}
