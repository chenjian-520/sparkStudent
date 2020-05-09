package com.aura.bigdata.spark.java.jvm;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Properties;

/**
 * Java引用分为：强引用>软引用>弱引用>虚引用
 *
 */
public class ReferenceTest {
    public static void main(String[] args) {
        System.out.println("===========强引用========");
        //强引用
        Person p = new Person();
        System.gc();//手动执行垃圾回收
        System.out.println(p);
        //软引用
        System.out.println("===========软引用========");
        SoftReference<Person> sp = new SoftReference<Person>(new Person());
        System.gc();
        System.out.println(sp.get());
        //弱引用
        System.out.println("===========弱引用========");
        WeakReference<Person> wp = new WeakReference<Person>(new Person());
        System.gc();
        System.out.println(wp.get());
        System.out.println("===========虚引用========");
        //虚引用
        ReferenceQueue<Person> referenceQueue = new ReferenceQueue<Person>();
        Person person = new Person();
        PhantomReference<Person> pp = new PhantomReference<Person>(person, referenceQueue);
        person = null;
        System.out.println(referenceQueue.poll());
        System.gc();
        System.out.println(pp.get());
        try {
            //gc后等1秒看结果
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(referenceQueue.poll());
        System.out.println("===================================");
        Properties properties = System.getProperties();
        for (Map.Entry<Object, Object> me : properties.entrySet()) {
            System.out.println(me.getKey() + "=" + me.getValue());
        }
        System.out.println("=================获取传递个JVM的参数=========================");
        System.out.println(System.getProperty("zookeeper.root.logger"));//-Dzookeeper.root.logger=INFO,stdout,R


        Person p1 = new Person();
        Person nP = p1;//
        p1 = null;
        System.out.println(nP);
    }
}

class Person {
    String name = "张三";

    @Override
    public String toString() {
        return name;
    }
}
