package cn.jxufe.czk.util;


import java.sql.*;

public class DBUtil {

    static String url = "jdbc:mysql://localhost:3306/spider"
            + "?useUnicode=true&"
            + "characterEncoding=utf-8&"
            + "serverTimezone=GMT%2B8&"
            + "useSSL=false";
    static Connection connection = null;
    static {
        try {
            Class<?> clazz = Class.forName("com.mysql.cj.jdbc.Driver");

        } catch (Exception e) {
        }
    }

    public static Connection getConnection() {


        try {
            connection = DriverManager.getConnection(url, "root", "root");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return connection;
    }


    public static void closeConnection(Statement statement, Connection connection) {
        try {
            if(statement!=null) {
                statement.close();
            }
            if(connection!=null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("关闭异常!");
        }
    }

    public static void closeConnection(PreparedStatement preparedStatement, Connection connection) {

        try {
            if(preparedStatement!=null) {
                preparedStatement.close();
            }
            if(connection!=null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("关闭异常!");
        }
    }

}
