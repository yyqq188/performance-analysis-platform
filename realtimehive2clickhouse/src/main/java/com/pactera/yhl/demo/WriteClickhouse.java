package com.pactera.yhl.demo;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.Config;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class WriteClickhouse {
    String address = Config.address;
    String user = Config.user;
    String password = Config.password;
    Connection connection;
    Statement statement;
    ResultSet results;
    String sql ;
    public WriteClickhouse() throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        //用的默认配置
        connection = DriverManager.getConnection(address);
//        connection = DriverManager.getConnection(address,user,password);
        statement = connection.createStatement();
    }

    public void write(String s) throws Exception {
        Object value = Transfor.transfor(s);
        Field[] declaredFields = value.getClass().getDeclaredFields();
        List<String> arrs = new ArrayList<>();
        System.out.println("======================");
        for(Field f:declaredFields){
            Object o = f.get(value);
            System.out.println(o);
            try{
                arrs.add(o.toString());
            }catch (Exception e){
                arrs.add("null");
            }
        }
        sql = String.format("insert into ldcode values (\'%s\')",
                String.join("\',\'",arrs));
        System.out.println(sql);
        statement.executeQuery(sql);

    }
}
