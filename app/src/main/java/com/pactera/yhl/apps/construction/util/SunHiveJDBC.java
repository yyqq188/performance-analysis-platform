package com.pactera.yhl.apps.construction.util;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.Lbcont;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.entity.source.T02salesinfok;
import com.pactera.yhl.sink.abstr.MyKafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Properties;

public class SunHiveJDBC {
    public static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static String url = "jdbc:hive2://10.5.2.145:10000/kl_umss";//端口默认10000
    public static String user = "hive";
    public static String password = "hive";
    public static Connection conn = null;
    private static Statement stmt = null;

    protected static KafkaProducer<String,String> producer;

    public static Statement Connection() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();
        return stmt;
    }

    public static void main(String[] args) throws Exception {

        //kafka的配置
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("bootstrap.servers","10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667");


        producer = new KafkaProducer<String, String>(kafkaProps);

        /*File file = new File("C:\\Users\\1\\Desktop\\lbcont.txt");
        FileWriter fw = new FileWriter(file.getName(),true);*/

        SunHiveJDBC.Connection();

//        String sql = "select * from kl_core.lcpol where contno = 'P2021310100506469'";
        String sql = "SELECT * from kl_umss.t02salesinfo_k where etl_dt like '2021-11-17%'";

        ResultSet resultSet = stmt.executeQuery(sql);


        T02salesinfok t02salesinfok = new T02salesinfok();
        while (resultSet.next()) {
            Field[] fields = t02salesinfok.getClass().getDeclaredFields();
            for (Field field : fields) {
                String value = resultSet.getString(field.getName());
                field.setAccessible(true);
                field.set(t02salesinfok,value);
            }
            producer.send(new ProducerRecord<>("tsy", "{\"data\":"+ JSON.toJSONString(t02salesinfok) +",\"meta\":{\"tableName\":\"kl_umss.t02salesinfo_k\"}}"));

        }



//        T01teaminfo t01teaminfo = new T01teaminfo();
//        while (resultSet.next()){
//            Field[] fields = t01teaminfo.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(t01teaminfo,value);
//            }
//            fw.write(JSON.toJSONString(t01teaminfo) + "\n");
//            fw.flush();
//        }


//        T01branchinfo t01branchinfo = new T01branchinfo();
//        while (resultSet.next()){
//            Field[] fields = t01branchinfo.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(t01branchinfo,value);
//            }
//            fw.write(JSON.toJSONString(t01branchinfo) + "\n");
//            fw.flush();
//        }
//        fw.close();


        /*Lcpol lcpol = new Lcpol();
        while (resultSet.next()){
            Field[] fields = lcpol.getClass().getDeclaredFields();
            for (Field field : fields) {
                String value = resultSet.getString(field.getName());
                field.setAccessible(true);
                field.set(lcpol,value);
            }
            System.out.println(JSON.toJSONString(lcpol));
            producer.send(new ProducerRecord<>("tsy", "{\"data\":"+ JSON.toJSONString(lcpol) +",\"meta\":{\"tableName\":\"kl_core.lcpol\"}}"));
//            fw.write(JSON.toJSONString(lcpol) + "\n");
//            fw.flush();
        }*/
//        fw.close();


//        Lccont lccont = new Lccont();
//        while (resultSet.next()){
//            Field[] fields = lccont.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(lccont,value);
//            }
//            fw.write(JSON.toJSONString(lccont) + "\n");
//            fw.flush();
//        }
//        fw.close();

//        Lbpol lbpol = new Lbpol();
//        while (resultSet.next()){
//            Field[] fields = lbpol.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(lbpol,value);
//            }
//            fw.write(JSON.toJSONString(lbpol) + "\n");
//            fw.flush();
//        }
//        fw.close();

//        Lpedoritem lpedoritem = new Lpedoritem();
//        while (resultSet.next()){
//            Field[] fields = lpedoritem.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(lpedoritem,value);
//            }
//            fw.write(JSON.toJSONString(lpedoritem) + "\n");
//            fw.flush();
//        }
//        fw.close();

//        Lbcont lbcont = new Lbcont();
//        while (resultSet.next()){
//            Field[] fields = lbcont.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(lbcont,value);
//            }
//            /*fw.write(JSON.toJSONString(lbcont) + "\n");
//            fw.flush();*/
//            producer.send(new ProducerRecord<>("tsy", JSON.toJSONString(lbcont)));
//        }
//        //fw.close();
    }

}
