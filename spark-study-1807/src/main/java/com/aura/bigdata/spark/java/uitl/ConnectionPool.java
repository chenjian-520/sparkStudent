package com.aura.bigdata.spark.java.uitl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * 自定义数据库连接池
 */
public class ConnectionPool {

    private static LinkedList<Connection> pool = new LinkedList<>();
    private ConnectionPool(){}
    private static int initialSize = 10;
    private static int maxActive = 50;
    private static int used = 0;
    private static String url = "jdbc:mysql://localhost:3306/test";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            for (int i = 0; i < initialSize; i++) {
                pool.add(DriverManager.getConnection(url, "root", "sorry"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        while(pool.isEmpty()) {//池子中的连接没了
            //重新创建，但是创建的最大的个数不能超过maxActive
        }
        Connection connection = pool.poll();
        used++;
        return connection;
    }

    public static void returnConnection(Connection connection) {
        used--;
        pool.push(connection);
    }
}
