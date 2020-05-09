package com.aura.bigdata.crawler;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.XPatherException;

import java.io.IOException;

public class Crawler {

    /**
     * 下载http网站数据-->httpclient
     *
     * @return
     */

    public String douwnload(String url) {
        //相当于浏览器
        HttpClient httpClient = HttpClients.createDefault();
         HttpUriRequest request = new HttpGet(url);
        try {
            //回车
            HttpResponse response = httpClient.execute(request);

            HttpEntity entity = response.getEntity();
            String s = EntityUtils.toString(entity);
//            System.out.println(s);
            return s ;
        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;
    }

    public Page parse(String content) {
        HtmlCleaner cleaner = new HtmlCleaner();
        TagNode root = cleaner.clean(content);
        try {
            Object[] objs = root.evaluateXPath("//*[@id=\"cb_post_title_url\"]");
            if(objs!=null || objs.length>0){
                TagNode title = (TagNode) objs[0];
                System.out.println(title.getText());

            }
        } catch (XPatherException e) {
            e.printStackTrace();
        }



        return null;
    }

    public void store() {

    }
}
