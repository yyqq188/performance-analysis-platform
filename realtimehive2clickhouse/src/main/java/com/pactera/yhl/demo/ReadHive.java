package com.pactera.yhl.demo;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple2;


import java.sql.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class ReadHive {
    PreparedStatement stmt;
    Connection con;
    List<String> fields;
    List<String> fieldTypes;
    String tableName;
    LinkedBlockingQueue<Object> queueCap;
    public ReadHive(String hiveTableName, LinkedBlockingQueue<Object> queueCap) throws SQLException, ClassNotFoundException {
        this.tableName = hiveTableName;
        this.queueCap = queueCap;
        init();
    }
    private void init() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Tuple2<List<String>, List<String>> fieldTuple = getFieldStr(tableName);
        fields = fieldTuple.getField(0);
        fieldTypes = fieldTuple.getField(1);


        String querySQL = String.join(" ",
                "select",String.join(",",fields),"from",tableName,"limit 10");
        System.out.println(querySQL);
        String url = "jdbc:hive2://prod-bigdata-pc10:2181,prod-bigdata-pc14:2181,prod-bigdata-pc15:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        con = DriverManager.getConnection(url);
        stmt = con.prepareStatement(querySQL);

    }

    public void run() throws Exception {
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
//            System.out.println(JSON.toJSONString(map));
            queueCap.put(JSON.toJSONString(map));
        }
    }

    private static Tuple2<List<String>,List<String>> getFieldStr(String tableName) throws SQLException {
        String querySQL = "desc " + tableName;
        String url = "jdbc:hive2://prod-bigdata-pc10:2181,prod-bigdata-pc14:2181,prod-bigdata-pc15:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        Connection con = DriverManager.getConnection(url);
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
