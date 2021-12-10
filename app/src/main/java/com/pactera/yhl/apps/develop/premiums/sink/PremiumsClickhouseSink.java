package com.pactera.yhl.apps.develop.premiums.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PremiumsClickhouseSink<T> extends RichSinkFunction<T> {
    String address = "jdbc:clickhouse://10.5.2.134:8123/default";
    //    String address = "jdbc:clickhouse://10.114.10.94:9191/default";
    String user = "default";
    String password = "default";
    Connection connection;
    Statement statement;
    ResultSet results;
    String sql ;
    String tableName ;

    public PremiumsClickhouseSink(String tableName){
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
        List<Object> arrs = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        for(Field f:declaredFields){
            Object o = f.get(value);
            sb.append(",");
            if("String".equals(f.getType().getSimpleName())){
                try{
                    arrs.add(o.toString());
                    sb.append("\'" +o.toString()+"\'");
                }catch (Exception e){
                    arrs.add("null");
                    sb.append("\'null\'");
                }

            }else if("Double".equals(f.getType().getSimpleName())){
                try{
                    arrs.add(Double.valueOf(o.toString()));
                    sb.append(Double.valueOf(o.toString()));
                }catch (Exception e){
                    arrs.add(0.00);
                    sb.append(0.00);
                }
            }
        }


//        sql = String.format("insert into %s values (\'%s\')",tableName,
//                String.join("\',\'",arrs));
        sql = String.format("insert into %s values (%s)",tableName,
                sb.toString().substring(1,sb.toString().length()));
        System.out.println("sql语句是 " + sql);
        statement.executeQuery(sql);

//        results = statement.executeQuery(sql);
//        ResultSetMetaData rsmd = results.getMetaData();


    }


    public static void main(String[] args) {
        Field[] fs = new AA().getClass().getDeclaredFields();
        for(Field f:fs){
            System.out.println(f.getType().getSimpleName());
        }

    }
    static class AA{
        public String a;
        public Double b;
    }
}
