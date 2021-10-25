package com.pactera.yhl.flinkck.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ClickhouseJDBCApp {
    public static void main(String[] args) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://39.106.163.125:8123/default";
        Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from city");
        while (resultSet.next()){
            String id = resultSet.getString("province");
            System.out.println(id);
        }
        resultSet.close();
        statement.close();
        connection.close();

    }
}
