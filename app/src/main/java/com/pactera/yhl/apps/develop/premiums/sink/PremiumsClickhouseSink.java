package com.pactera.yhl.apps.develop.premiums.sink;

import com.google.common.collect.Sets;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

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
        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();
        Set<String> fields = new HashSet<>();
        for(Field f:declaredFields){
            if(f.getName().equals("fieldName")){
                for(String fieldName:f.get(value).toString().split(",")){
                    if(fieldName.equals("columnName") ||
                    fieldName.equals("fieldName") ||
                    fieldName.equals("valueField")){
                        continue;
                    }
                    fields.add(fieldName);
                }
            }
        }

        System.out.println("fields is == "+fields.toString());
        for(Field f:declaredFields){
            Object o = f.get(value);

            if(fields.contains(f.getName())){
                sbKey.append(",");
                sbValue.append(",");

                if("String".equals(f.getType().getSimpleName())){
                    try{
                        sbKey.append(f.getName());
                        sbValue.append("\'" +o.toString()+"\'");

                    }catch (Exception e){
                        sbValue.append("\'null\'");
                    }

                }else if("Double".equals(f.getType().getSimpleName())){
                    try{
                        sbKey.append(f.getName());
                        sbValue.append(Double.valueOf(o.toString()));
                    }catch (Exception e){
                        sbValue.append(0.00);
                    }
                }
            }
        }


//        sql = String.format("insert into %s values (\'%s\')",tableName,
//                String.join("\',\'",arrs));
//        sql = String.format("insert into %s values (%s)",tableName,
//                sb.toString().substring(1,sb.toString().length()));
        sql = String.format("insert into %s (%s) values (%s)",tableName,
                sbKey.toString().toUpperCase().substring(1,sbKey.toString().length()),
                sbValue.toString().substring(1,sbValue.toString().length()));

        System.out.println("sql语句是 " + sql);
        statement.executeQuery(sql);


    }





    /**
     * 这个方法会有严重问他，会把之前有值的字段设置为0,从而造成数据不对，
     * 应该更新指定更新的字段，不需要更新的字段不应该覆盖更新
     */
//    @Override
//    public void invoke(T value, Context context) throws Exception {
//        Field[] declaredFields = value.getClass().getDeclaredFields();
//        List<Object> arrs = new ArrayList<>();
//        StringBuffer sb = new StringBuffer();
//        for(Field f:declaredFields){
//            Object o = f.get(value);
//            sb.append(",");
//            if("String".equals(f.getType().getSimpleName())){
//                try{
//                    arrs.add(o.toString());
//                    sb.append("\'" +o.toString()+"\'");
//                }catch (Exception e){
//                    arrs.add("null");
//                    sb.append("\'null\'");
//                }
//
//            }else if("Double".equals(f.getType().getSimpleName())){
//                try{
//                    arrs.add(Double.valueOf(o.toString()));
//                    sb.append(Double.valueOf(o.toString()));
//                }catch (Exception e){
//                    arrs.add(0.00);
//                    sb.append(0.00);
//                }
//            }
//        }
//
//
////        sql = String.format("insert into %s values (\'%s\')",tableName,
////                String.join("\',\'",arrs));
//        sql = String.format("insert into %s values (%s)",tableName,
//                sb.toString().substring(1,sb.toString().length()));
//        System.out.println("sql语句是 " + sql);
//        statement.executeQuery(sql);
//
////        results = statement.executeQuery(sql);
////        ResultSetMetaData rsmd = results.getMetaData();
//
//
//    }
}
