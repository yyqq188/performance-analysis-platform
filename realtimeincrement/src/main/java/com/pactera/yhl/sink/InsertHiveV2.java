package com.pactera.yhl.sink;

import com.pactera.yhl.Util;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InsertHiveV2<OUT> extends RichSinkFunction<OUT> {
    PreparedStatement stmt;
    Connection con;
    @Override
    public void open(Configuration parameters) throws Exception {
        Map<String, String> configMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();

        Class.forName(configMap.get("driver"));
        con = DriverManager.getConnection(configMap.get("url"));
//        stmt = con.createStatement();
        stmt = con.prepareStatement(
                "insert into table testyhl.ldcode2 select ?,?,?,?,?,?,?,?,?,?,?,?");
    }

    @Override
    public void close() throws Exception {
        stmt.close();
        con.close();
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {

        Map<String,Object> map=new HashMap<String,Object>();
        Field[] declaredFields = value.getClass().getDeclaredFields();
        int num = 0;
        for(Field f:declaredFields){
            num +=1 ;
            String fieldName = f.getName();
            map.put("value", value);
            String expression = "value.get" + Util.LargerFirstChar(fieldName) + "()";
            Object value1 = Util.convertToCode(expression, map);
            try {
                stmt.setString(num,String.valueOf(value1));
            } catch (SQLException ex) {
                ex.printStackTrace();
            }

        }


        stmt.execute();

    }
}
