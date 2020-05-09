package com.aura.bigdata;

import java.util.ArrayList;
import java.util.List;

public class 交集和差集 {
    public static void main(String[] args) {
        List<String> list1 =new ArrayList();
        list1.add("1111");
        list1.add("2222");
        list1.add("3333");

        List list2 =new ArrayList();
        list2.add("3333");
        list2.add("4444");
        list2.add("5555");


        //无重复并集
        list1.removeAll(list2);
        System.out.println("list1: " + list1);
        System.out.println("list2: " + list2);
    }
}
