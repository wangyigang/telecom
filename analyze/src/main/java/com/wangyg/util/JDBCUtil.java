package com.wangyg.util;


import org.apache.hadoop.util.PlatformName;

import java.sql.*;

/**
 * 使用单例模式，构造静态内部类
 */
public class JDBCUtil {
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://hadoop102:3306/db_telecom?useUnicode=true&characterEncoding=UTF-8";
    private static final String USER_NAME = "root";
    private static final String PASSWD = "1";

    //构造器私有化
    private JDBCUtil(){

    }
    //单例设计模式有两点要求：一：构造器私有化 二：定义静态属性
    private static  Connection connection = null;

    //获取连接
    private static Connection getConnection(){
        Connection connection = null;
        try {
            //加载驱动
            Class.forName(DRIVER_CLASS);
            //获取连接
            connection = DriverManager.getConnection(URL, USER_NAME, PASSWD);


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return  connection;
    }

    public static  void clost(ResultSet resultSet,
                              PreparedStatement preparedStatement,
                              Connection connection){
        if(resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(preparedStatement!=null){
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    //获取单例连接
    public static Connection getInstance(){
        if(connection == null){
            synchronized (JDBCUtil.class){
                if(connection == null){
                    connection = getConnection();
                }
            }
        }
        return connection;
    }
}
