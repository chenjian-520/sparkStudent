package com.aura.bigdata.spark.java.uitl;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;


/**
 * 工具类
 * 	所有的连接和资源释放
 */

public class JDBCUtil_cj {


    private List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();

        //静态代码块，在程序编译的时候执行
        static {
            //创建Properties对象
            Properties properties = new Properties();
            //获取文件输入流
            InputStream is = JDBCUtil_cj.class.getClassLoader().getResourceAsStream("jdbc.properties");
            try {
                //加载输入流
                properties.load(is);
                is.close();
                //加载mysql驱动
                Class.forName(properties.getProperty("driver"));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

    //关闭资源
    public static void closeRes(Connection conn,PreparedStatement ps,ResultSet rs) {
        try {
            if(rs != null){
                rs.close();
            }
            if(ps != null){
                ps.close();
            }
            if(conn != null){
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //获取连接
    public static Connection getConnection(){
        Connection conn = null;
        Properties properties = new Properties();
        InputStream is = JDBCUtil_cj.class.getClassLoader().getResourceAsStream("jdbc.properties");
        try {
            properties.load(is);
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(
                    properties.getProperty("url"),
                    properties.getProperty("username"),
                    properties.getProperty("password"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void main(String[] args) {
        Connection connection = getConnection();
        String sql = "select * from dept where deptno = ?";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setInt(1,10);
             resultSet = preparedStatement.executeQuery();
            ResultSetMetaData md = resultSet.getMetaData(); //获得结果集结构信息,元数据
            int columnCount = md.getColumnCount();   //获得列数
            while (resultSet.next()) {
                Map<String,Object> rowData = new HashMap<String,Object>();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnName(i), resultSet.getObject(i));
                }
                JDBCUtil_cj cj = new JDBCUtil_cj();
                cj.list.add(rowData);


                for (Map.Entry<String, Object> entry : rowData.entrySet()) {

                    System.out.println(entry.getKey()+"==="+entry.getValue());
                }
                System.out.println(rowData);
                System.out.println(cj.list);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        JDBCUtil_cj.closeRes(connection,preparedStatement,resultSet);

    }
}
