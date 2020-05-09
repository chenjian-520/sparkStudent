package com.aura.bigdata.crawler;



/**
 * 爬虫的三个步骤
 * 下载
 * 解析
 * 存储
 */
public class CrawlerTest {
    public static void main(String[] args) {
        Crawler crawler = new Crawler();
        //下载
        String url = "https://www.cnblogs.com/jfl-xx/p/8024596.html";
        String content = crawler.douwnload(url);
        //解析
        Page page = crawler.parse(content);
        //存储
        crawler.store();
    }
}
