package com.aura.bigdata.spark.java.jvm;

/**

 */
public class ConstantPool {

    public static void main(String[] args) {

        String str1=new String("hello");//

        String str2=new String("hello");

       // Thread.sleep(10000);

        //==比较的是两个对象的内存地址值，如果是两个数值，比较的是数值本身
        System.out.println(str1==str2);
        //equals在Object类中比较的内存地址值，但是在String中覆盖了该方法，先比较内存地址值，在比较两个字符序列是否相等
        System.out.println(str1.equals(str2));

        String str3 = "hello";
        String str4 = "hello";
        System.out.println("str3==str4? " + (str3==str4));//true
        System.out.println("str3.equals(str4)? " + str3.equals(str4));//true

        System.out.println("------------------------------");
        Integer i1 = 4;
        Integer i2 = 4;
        System.out.println(i1 == i2);//true true
        Integer i3 = new Integer(3);
        Integer i4 = new Integer(3);
        System.out.println(i3 == i4);//true(×) false
        Integer i5 = new Integer(256);
        Integer i6= new Integer(256);
        System.out.println(i5 == i6);//false    false
        Integer i7 = 256;
        Integer i8 = 256;
        System.out.println(i7 == i8);//false false
        /**
         * jvm对于常用的一些数字做了一些优化，在一个byte范围内的数字[-128, 127]之间的数字的创建都是在常量池中创建
         * 缓存起来，后面直接从常亮池中进行引用即可。
         */
    }

}
