package com.pactera.yhl.source;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import lombok.SneakyThrows;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.*;

public class MyHiveSource extends RichSourceFunction<String> {
    PreparedStatement stmt;
    Connection con;
    List<String> fields;
    String tableName;
    public MyHiveSource(String hiveTableName){
        this.tableName = hiveTableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();

        Class.forName(parameterTool.get("driver"));
        fields = getFieldStr(tableName, parameterTool);
        String querySQL = String.join(" ",
                "select",String.join(",",fields),"from",tableName);
        System.out.println(querySQL);
        con = DriverManager.getConnection(parameterTool.get("url"));
        stmt = con.prepareStatement(querySQL);



    }



    @Override
    public void run(SourceContext<String> ctx) throws Exception {


        ResultSet res = stmt.executeQuery();

        while(res.next()){
            Map<String,String> mapData = new HashMap<>();
            Map<String,Object> map = new HashMap<>();
            Map<String,Object> maptableName = new HashMap<>();
            maptableName.put("tableName",tableName);
            maptableName.put("fields",fields);
            map.put("meta",maptableName);
            //fastjson当值为空的时候，key就不显示，为了避免，所以要加判断
            for (int i = 0; i < fields.size(); i++) {
                if(Objects.isNull(res.getString(i + 1))){
                    mapData.put(fields.get(i), "");
                }else{
                    mapData.put(fields.get(i), res.getString(i + 1));
                }

            }
            map.put("data",mapData);
            ctx.collect(JSON.toJSONString(map));
        }

    }

    @SneakyThrows
    @Override
    public void cancel() {
        if(stmt != null){
            stmt.close();
        }
        if(con != null){
            con.close();
        }

    }

    private static List<String> getFieldStr(String tableName, ParameterTool prop) throws SQLException {
        String querySQL = "desc " + tableName;
        Connection con = DriverManager.getConnection(prop.get("url"));
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery(querySQL);
        List<String> fields = new ArrayList<>();
        while(res.next()){
            fields.add(res.getString(1));
        }

        return fields;

    }
}
