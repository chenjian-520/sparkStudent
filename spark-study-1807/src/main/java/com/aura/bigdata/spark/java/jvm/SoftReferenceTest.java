package com.aura.bigdata.spark.java.jvm;

import javax.sound.midi.SysexMessage;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;

public class SoftReferenceTest {
    public static void main(String[] args) {
        System.out.println("========内存溢出前的软引用=============>");
        SoftReference<Person> sr = new SoftReference<Person>(new Person());
        System.gc();
        System.out.println(sr.get());

        try {
            List<SoftReferenceTest> list = new ArrayList<SoftReferenceTest>();
            while (true) {
                list.add(new SoftReferenceTest());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("========内存溢出后的软引用=============>");
            System.out.println(sr.get());
        }
    }
}
