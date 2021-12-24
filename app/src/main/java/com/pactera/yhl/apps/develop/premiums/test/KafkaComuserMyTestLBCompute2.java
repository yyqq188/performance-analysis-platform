package com.pactera.yhl.apps.develop.premiums.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaComuserMyTestLBCompute2 {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers",
                "10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group14");

        // 可选设置属性
        props.put("enable.auto.commit", "true");
        // 自动提交offset,每1s提交一次
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","latest ");
        props.put("client.id", "zy_client_id");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("APPLICATION_GENERAL_RESULT_RT"));
        Map<String,String> maps = new ConcurrentHashMap<String,String>();

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                String key = "";
                String v = "";
                String message = record.value()
                        .replace("ApplicationGeneralResultWithColumnName","")
                        .replace("(","")
                        .replace(")","");

                System.out.println("message = " + message);
//                if(maps.containsKey(key)){
//                    maps.put(key,String.valueOf(Double.valueOf(v) + Double.valueOf(maps.get(key))));
//                }else{
//                    maps.put(key,v);
//                }
//
//                System.out.println("maps = " + maps);


            });
        }
  }
}