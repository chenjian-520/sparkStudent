package com.aura.bigdata.spark.java.jvm;

public class FinalizeObj {
    public static FinalizeObj obj;

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        System.out.println("FinalizeObj finalize called !!!");
        obj = this;//在finalize方法中复活对象
    }

    @Override
    public String toString() {
        return "I am FinalizeObj";
    }

    public static void main(String[] args) throws InterruptedException {
        obj = new FinalizeObj();
        obj = null; //将obj设为null
        System.gc();//垃圾回收

        Thread.sleep(1000);//
        if(obj == null) {
            System.out.println("obj is null");
        } else {
            System.out.println("obj is alive");
        }

        System.out.println("第2次调用gc后");
        obj = null;//由于obj被复活，此处再次将obj设为null
        System.gc();//再次gc
        Thread.sleep(1000);
        if(obj == null) {
            //对象的finalize方法仅仅会被调用一次，所以可以预见再次设置obj为null后，obj会被垃圾回收，该语句会被调用
            System.out.println("obj is null");
        } else {
            System.out.println("obj is alive");
        }
    }
}
