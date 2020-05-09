package com.aura.bigdata;

import java.util.Arrays;

public class AAA {
    public static void main(String[] args) {
        String line = "27.19.74.143##2016-05-30 17:38:20##GET /static/image/common/faq.gif HTTP/1.1##200##1127";

        System.out.println(line.substring(0, line.indexOf("##")));
        System.out.println(line.substring(line.indexOf("##")));

    }
}
