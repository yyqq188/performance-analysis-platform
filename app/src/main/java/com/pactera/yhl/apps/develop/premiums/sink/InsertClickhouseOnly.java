package com.pactera.yhl.apps.develop.premiums.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.util.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class InsertClickhouseOnly<T> extends RichSinkFunction<T> {
//    String address = "jdbc:clickhouse://10.5.2.134:8123/default";
    String address = "jdbc:clickhouse://10.5.2.134:8123/kl_base";
    String user = "default";
    String password = "default";
    Connection connection;
    Statement statement;
    ResultSet results;
    String sql;
    String tableName;

    public InsertClickhouseOnly(String tableName){
        this.tableName = tableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
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

        //开始插入到clickhouse
        Field[] declaredFields = value.getClass().getDeclaredFields();
        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();

        for(Field f:declaredFields){
            Object o = f.get(value);
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
        sql = String.format("insert into %s (%s) values (%s)",tableName,
                sbKey.toString().toUpperCase().substring(1,sbKey.toString().length()),
                sbValue.toString().substring(1,sbValue.toString().length()));
        System.out.println("sqlValue123 = " + sql);
        statement.executeQuery(sql);
    }
}