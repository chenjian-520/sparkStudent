package com.aura.bigdata.spark.java.core.p3;

import com.aura.bigdata.spark.java.entity.Person;

import java.io.*;

/**
 * java的序列化和反序列化对象操作说明
 */
public class SerializedAndDeserializedObjOps {
    public static void main(String[] args) throws Exception {
        String filepath = "objs/person.txt";
//        writeObj2File(filepath);
//        readObjFromFile(filepath);
        writeObj2File2(filepath);
        readObjFromFile2(filepath);
        return;
    }

    /**
     * 为了解决这个EOFException(end-of-file)异常，两种方案：
     *  第一：自动在对象写完之后添加一个终止符
     *  第二：在写对象的时候，多个对象合并成为一个写入文件
     *      一个对象数组
     * @param filepath
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static void readObjFromFile2(String filepath) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filepath));
        Object obj = ois.readObject();
        if(obj instanceof Person[]) {
            Person[] ps = (Person[])obj;
            for (Person p : ps) {
                System.out.println(p);
            }
        }
        ois.close();
    }

    private static void writeObj2File2(String filepath) throws IOException {
        Person p1 = new Person();
        p1.setName("邓仿");
        p1.setAge(18);
        Person p2 = new Person();
        p2.setName("刘德根");
        p2.setAge(26);
        Person[] ps = {p1, p2};
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filepath));
        oos.writeObject(ps);
        oos.close();
    }

    private static void readObjFromFile(String filepath) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filepath));
//        Object obj = ois.readObject();
//        if(obj instanceof Person) {
//            Person p = (Person)obj;
//            System.out.println(p);
//        }
//        obj = ois.readObject();
//        if(obj instanceof Person) {
//            Person p = (Person)obj;
//            System.out.println(p);
//        }
        Object obj = null;
        while ((obj = ois.readObject()) != null) {
            Person p = (Person)obj;
            System.out.println(p);
        }
        ois.close();
    }


    private static void writeObj2File(String filepath) throws IOException {
        Person p1 = new Person();
        p1.setName("邓仿");
        p1.setAge(18);
        Person p2 = new Person();
        p2.setName("刘德根");
        p2.setAge(26);
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filepath));

        oos.writeObject(p1);
        oos.writeObject(p2);

        oos.close();
    }

}
