package com.pactera.yhl.hivekafka2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class DirectHiveToKafka {

    public static void main(String[] args) throws Exception {
        KafkaProducer producer = KafkaClient.getProducer();
        System.out.println(producer);
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream("D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\dws\\src\\main\\resources\\conf.properties"));
            prop.load(in);
//            Float.valueOf("");
        }catch (Exception e){
            e.printStackTrace();
        }

        Class.forName(prop.getProperty("driver"));
        String querySQL = prop.getProperty("sql");
        Connection con = DriverManager.getConnection(prop.getProperty("url"));
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery(querySQL);
        while(res.next()){
            System.out.println(res.getString(1));
            producer.send(new ProducerRecord("lupol",res.getString(1))).get();
        }
    }
}
