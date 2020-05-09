package com.aura.bigdata

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids


object CuratorTest {

    def main(args: Array[String]): Unit = {
        val client = {//zookeeper
            val client = CuratorFrameworkFactory.builder()
                    .connectString("bigdata01:2181,bigdata02:2181,bigdata03:2181")
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build()
            //CuratorFramework要想使用，必须start
            client.start()
            client
        }
        //创建目录
        val path = "/test1/test"
        if(client.checkExists().forPath(path) == null) {
            val haha = client.create().creatingParentsIfNeeded().forPath(path, "hahah".getBytes())
            println("创建成功吗？")
        }
        import scala.collection.JavaConversions._
        //获取子节点的列表
        val list = client.getChildren.forPath("/test1")
        val parent = "/test1"
        list.foreach(child => {
            val cpath = s"${parent}/${child}"
            val data = new String(client.getData.forPath(cpath))
            println(s"${cpath}对应的数据：" + data)
        })

        client.close()
    }
}
