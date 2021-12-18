package com.pactera.yhl.source;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
@Slf4j
public class MyHiveSource extends RichSourceFunction<String> {
    PreparedStatement stmt;
    Connection con;
    List<String> fields;
    List<String> fieldTypes;
    String tableName;
    public MyHiveSource(String hiveTableName){
        this.tableName = hiveTableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        Class.forName(parameterTool.get("driver"));
        System.out.println("连接成功");
        Tuple2<List<String>, List<String>> fieldTuple = getFieldStr(tableName,parameterTool);
        fields = fieldTuple.getField(0);
        fieldTypes = fieldTuple.getField(1);
        String querySQL = String.join(" ",
                "select",String.join(",",fields),"from",tableName,"limit 10");
        System.out.println(querySQL);
        log.info(querySQL);
        con = DriverManager.getConnection(parameterTool.get("url"));
        stmt = con.prepareStatement(querySQL);
    }
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ResultSet res = stmt.executeQuery();
        while(res.next()){
            Map<String,Object> mapData = new HashMap<>();
            Map<String,Object> map = new HashMap<>();
            Map<String,Object> maptableName = new HashMap<>();
            maptableName.put("tableName",tableName);
            maptableName.put("fields",fields);
            maptableName.put("fieldTypes",fieldTypes);
            map.put("meta",maptableName);
            //fastjson当值为空的时候，key就不显示，为了避免，所以要加判断
            for (int i = 0; i < fields.size(); i++) {
                if(fieldTypes.get(i).equals("string")){
                    if(Objects.isNull(res.getString(i + 1))){
                        mapData.put(fields.get(i), "null");
                    }else{
                        mapData.put(fields.get(i), res.getString(i + 1));
                    }
                }else if(fieldTypes.get(i).contains("decimal")){
                    if(Objects.isNull(res.getBigDecimal(i + 1))){
                        mapData.put(fields.get(i), 0);
                    }else{
                        mapData.put(fields.get(i), res.getBigDecimal(i + 1));
                    }

                }else if(fieldTypes.get(i).equals("bigint")){
                    if(Objects.isNull(res.getInt(i + 1))){
                        mapData.put(fields.get(i), 0);
                    }else{
                        mapData.put(fields.get(i), res.getInt(i + 1));
                    }
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

    private static Tuple2<List<String>, List<String>> getFieldStr(String tableName, ParameterTool prop) throws SQLException {
        String querySQL = "desc " + tableName;
        Connection con = DriverManager.getConnection(prop.get("url"));
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery(querySQL);
        List<String> fields = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();
        while(res.next()){
            fields.add(res.getString(1));
            fieldTypes.add(res.getString(2));
        }

        return Tuple2.of(fields,fieldTypes);

    }
}
