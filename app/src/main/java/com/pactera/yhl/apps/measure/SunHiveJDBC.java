package com.pactera.yhl.apps.measure;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.*;
import com.pactera.yhl.entity.source.Lccont;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


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
    public static KafkaProducer producer = null;
    public static int i = 1;

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

        producer = new KafkaProducer<>(kafkaProps);

        SunHiveJDBC.Connection();
//        String sql = "select * from kl_umss.t02salesinfo_k where sales_id in('920108000032','920008000044','920008000045','920008000047','920008000050','920008000080','920008000092','920108000020','920108000033','920108000051','920108000036','920108000040','920108000041','920108000038','920108000039','920108000037','920008000051','920008000057','920008000069','920008000072','920008000074','920008000085','920108000010','920108000035','920108000043','920108000042','920108000048','920108000049','920108000047','920108000044','920108000045')";

//        String sql = "select * from t01teaminfo where versionid = '2021' and channel_id = '08'";
//        String sql = "select * from t01branchinfo";

//        String sql = "select * from kl_core.lcpol where SUBSTR(signdate,1,10) = '2021-09-30'";
//        String sql = "select * from kl_core.lbpol where SUBSTR(signdate,1,10) = '2021-09-30'";

        String sql = "select * from kl_core.lccont where SUBSTR(signdate,1,10) = '2021-09-30'";
//        String sql = "select * from kl_core.lbcont where SUBSTR(signdate,1,10) = '2021-09-30'";

//        String sql = "select * from kl_core.lbpol lbp left join kl_core.lpedoritem lpd on lbp.contno = lpd.contno and lbp.edorno = lpd.edorno where SUBSTR(lpd.modifydate,1,10) = '2021-09-30'";
//        String sql = "select * from kl_core.lpedoritem where SUBSTR(modifydate,1,10) = '2021-09-30'";

//        String sql = "select * from kl_core.lbcont lbc left join kl_core.lpedoritem lpd on lbc.contno = lpd.contno and  lbc.edorno = lpd.edorno where SUBSTR(lpd.modifydate,1,10) = '2021-09-30'";

        ResultSet resultSet = stmt.executeQuery(sql);


//        T02salesinfok t02salesinfok = new T02salesinfok();
//        while (resultSet.next()) {
//            Field[] fields = t02salesinfok.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(t02salesinfok,value);
//            }
//            t02salesinfok.setStat("1");
//            producer.send(new ProducerRecord<>("suntestsalesT", JSON.toJSONString(t02salesinfok)));
//            System.out.println(i++);
//        }



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


//        Lcpol lcpol = new Lcpol();
//        while (resultSet.next()){
//            Field[] fields = lcpol.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(lcpol,value);
//            }
//            producer.send(new ProducerRecord<>("suntestlcp", JSON.toJSONString(lcpol)));
////            System.out.println(JSON.toJSONString(lcpol));
//            System.out.println(i++);
//        }


        Lccont lccont = new Lccont();
        while (resultSet.next()){
            Field[] fields = lccont.getClass().getDeclaredFields();
            for (Field field : fields) {
                String value = resultSet.getString(field.getName());
                field.setAccessible(true);
                field.set(lccont,value);
            }
            producer.send(new ProducerRecord<>("suntestlccont", JSON.toJSONString(lccont)));
            System.out.println(i++);
        }


//        Lbpol lbpol = new Lbpol();
//        while (resultSet.next()){
//            Field[] fields = lbpol.getClass().getDeclaredFields();
//            for (Field field : fields) {
//                String value = resultSet.getString(field.getName());
//                field.setAccessible(true);
//                field.set(lbpol,value);
//            }
//            producer.send(new ProducerRecord<>("suntestlbp", JSON.toJSONString(lbpol)));
//            System.out.println(i++);
//        }


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
//            producer.send(new ProducerRecord<>("topic", JSON.toJSONString(lbcont)));
//        }



    }

}
