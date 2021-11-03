package com.pactera.yhl.hivekafka3;

import com.google.gson.Gson;
import com.pactera.yhl.hivekafka2.KafkaClient;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class DirectHiveToKafka implements Runnable{
    String topic;
    String tableName;
    Properties prop;
    String limitNum;
    public DirectHiveToKafka(String topic,String tableName,Properties prop,String limitNum){
        this.topic = topic;
        this.tableName = tableName;
        this.prop = prop;
        this.limitNum = limitNum;
    }

    @SneakyThrows
    @Override
    public void run() {
        KafkaProducer producer = KafkaClient.getProducer();
        Class.forName(prop.getProperty("driver"));
        List<String> fields = getFieldStr(tableName, prop);
        String querySQL = String.join(" ",
                "select",String.join(",",fields),"from",tableName,"limit",limitNum);
        System.out.println(querySQL);
        Connection con = DriverManager.getConnection(prop.getProperty("url"));
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery(querySQL);

        while(res.next()){
            Gson gson = new Gson();
            Map<String,String> mapData = new HashMap<>();
            Map<String,Object> map = new HashMap<>();
            Map<String,String> maptableName = new HashMap<>();
            maptableName.put("tableName",tableName);
            map.put("meta",maptableName);
            for (int i = 0; i < fields.size(); i++) {
                mapData.put(fields.get(i), res.getString(i + 1));
            }
            map.put("data",mapData);
            producer.send(new ProducerRecord(topic,gson.toJson(gson.toJson(map)))).get();
        }
    }

    private static List<String> getFieldStr(String tableName,Properties prop) throws SQLException {
        String querySQL = "desc " + tableName;
        System.out.println(querySQL);
        Connection con = DriverManager.getConnection(prop.getProperty("url"));
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery(querySQL);
        List<String> fields = new ArrayList<>();
        while(res.next()){
            fields.add(res.getString(1));
        }

        return fields;

    }
}
