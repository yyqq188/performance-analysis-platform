package com.pactera.yhl.sink;

import com.pactera.yhl.Util;
import com.pactera.yhl.entity.KLEntity;
import com.pactera.yhl.entity.Lbpol;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class InsertHive<OUT> extends RichSinkFunction<OUT> {
    Statement stmt;
    Connection con;
    @Override
    public void open(Configuration parameters) throws Exception {
        Map<String, String> configMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();

        Class.forName(configMap.get("driver"));
        con = DriverManager.getConnection(configMap.get("url"));
        stmt = con.createStatement();
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {

        Map<String,Object> map=new HashMap<String,Object>();
        Field[] declaredFields = value.getClass().getDeclaredFields();
        List<String> StrLists = Arrays.stream(declaredFields).map(
                        e -> {
                            String fieldName = e.getName();
                            map.put("value", value);
                            String expression = "value.get" + Util.LargerFirstChar(fieldName) + "()";
                            Object value1 = Util.convertToCode(expression, map);
                            return String.valueOf(value1);

                        })
                .collect(Collectors.toList());

        String strs = String.join("','", StrLists);
        strs = "'" + strs +"'";
        System.out.println("strs ---- " + strs);

        String querySQL = String.format("insert into table testyhl.ldcode2 select %s",strs);
        System.out.println("querySQL ---- "+querySQL);
        stmt.execute(querySQL);

    }
    @Override
    public void close() throws Exception {
        stmt.close();
        con.close();
    }
}
