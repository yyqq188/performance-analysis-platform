package com.pactera.yhl.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public  class MyClickhouseSink<T> extends RichSinkFunction<T> {
    String address = "jdbc:clickhouse://10.5.2.134:8123/default";
//    String address = "jdbc:clickhouse://10.114.10.94:9191/default";
    String user = "kl";
    String password = "kl@123";
    Connection connection;
    Statement statement;
    ResultSet results;
    String sql ;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

//        Properties properties = new Properties();
//        properties.setProperty("apache_buffer_size","655350"); //65535
//        properties.setProperty("buffer_size","655350"); //65535
//        properties.setProperty("MAX_TOTAL","20000");  //10000
//        connection = DriverManager.getConnection(address,properties);
        //用的默认配置
        connection = DriverManager.getConnection(address);
//        connection = DriverManager.getConnection(address,user,password);

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
        System.out.println(value);

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
        sql = String.format("insert into ldcode values (\'%s\')",
                String.join("\',\'",arrs));
        System.out.println(sql);
        statement.executeQuery(sql);

//        results = statement.executeQuery(sql);
//        ResultSetMetaData rsmd = results.getMetaData();


    }

}
