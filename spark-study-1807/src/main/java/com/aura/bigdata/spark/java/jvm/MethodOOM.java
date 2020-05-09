package com.aura.bigdata.spark.java.jvm;

import java.util.ArrayList;
import java.util.List;

/**
 * jdk1.8以前：VM Args：-XX:PermSize=2m -XX:MaxPermSize=2m
 * jdk1.8以后：-XX:MetaspaceSize=2m -XX:MaxMetaspaceSize=2m
 */
public class MethodOOM {
	//永久代内存异常
    public static void main(String[] args) {

        //保持引用，防止自动垃圾回收
        List<Student> list = new ArrayList<Student>();

        int i = 0;

        while(true){
            //通过intern方法向常量池中手动添加常量
            Student s = new Student("张三", 12);
            list.add(s);
        }
    }

}
class Student {
    private String name;
    private int age;

    public Student(String name, int age) {
        this.name = name;
        this.age = age;
    }
}
