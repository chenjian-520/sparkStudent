package com.aura.bigdata.spark.java.jvm;


import java.util.Random;

public class Demo {
    public static void main(String[] args) {
        while (true){
            try {
                Thread.sleep(500);
                Demo d=new Demo();
               d.getHello(new Random().nextInt());
              d.getMsg(new Random().nextInt());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    private String  getHello(int i){
        try {
            Thread.sleep(5);
            return "hello"+i;
        }catch (InterruptedException e) {
            return "error";
        }
    }
    private String  getMsg(int i){
            return "msg"+i;
    }
}
