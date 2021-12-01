package com.pactera.yhl.sink;

import com.pactera.yhl.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
@Slf4j
public  class MyClickhouseSink<T> extends RichSinkFunction<T> {
    String address = Config.address;
    String user = Config.user;
    String password = Config.password;
    Connection connection;
    Statement statement;
    ResultSet results;
    String sql ;
    String tableName;
    public MyClickhouseSink(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

//        Properties properties = new Properties();
//        properties.setProperty("apache_buffer_size","655350"); //65535
//        properties.setProperty("buffer_size","655350"); //65535
//        properties.setProperty("MAX_TOTAL","20000");  //10000
//        connection = DriverManager.getConnection(address,properties);
        //用的默认配置
//        connection = DriverManager.getConnection(address);
        connection = DriverManager.getConnection(address,user,password);
        statement = connection.createStatement();
    }

    @Override
    public void close() throws Exception {

        if(results!=null){
            results.close();
        }
        if(statement!=null){
            statement.close();
        }
        if(connection!=null){
            connection.close();
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        Field[] declaredFields = value.getClass().getDeclaredFields();
        List<String> arrs = new ArrayList<>();
        for(Field f:declaredFields){
            Object o = f.get(value);
            System.out.println(o);
            try{
                arrs.add(o.toString());
            }catch (Exception e){
                arrs.add("null");
            }
        }
        sql = String.format("insert into %s values (\'%s\')",
                tableName,
                String.join("\',\'",arrs));
        System.out.println(sql);
        statement.executeQuery(sql);

    }

}
