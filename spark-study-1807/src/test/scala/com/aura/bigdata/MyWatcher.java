package com.aura.bigdata;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 自定义zookeeper目录的监听器
 * 监听/test1目录下面的变化
 */
public class MyWatcher implements Watcher {
    private String parent = "/test";
    private CuratorFramework client;
    private List<String> children;
    public MyWatcher() {
        init();
        try {
            //拿到初始化之后的节点下面的所有的子节点信息
            children = client.getChildren().usingWatcher(this).forPath(parent);
            System.out.println("目录初始化之后的节点信息：" + children);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init(){
        client = CuratorFrameworkFactory.builder()
                .connectString("bigdata01:2181,bigdata02:2181,bigdata03:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();
    }

    /**
     * 当该watcher监听的目录发生变化之后，就会调用该方法
     * A-B={在B中没有在A中出现的内容} 差集（B去除A和B的交集）
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        System.out.println("目录发生变化，我被调用啦~");
        try {//节点发生变化之后的所有的子节点信息
            List<String> newChildren = client.getChildren().usingWatcher(this).forPath(parent);
            //只要newChildren中出现，在children没有的内容就是新增
            //在children有，而在newChildren没有的内容就是较少
            if(newChildren.size() > children.size()) {//新增
                for(String child : newChildren) {
                    if(!children.contains(child)) {
                        System.out.println("新增的节点：" + child);
                    }
                }
            } else {//减少
                for(String child : children) {
                    if(!newChildren.contains(child)) {
                        System.out.println("减少的节点：" + child);
                    }
                }
            }

            children = newChildren;//当前最新的子节点状态
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() {
        while(true){}
    }

    public static void main(String[] args) {
        new MyWatcher().start();
    }
}
