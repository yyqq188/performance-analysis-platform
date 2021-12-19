package com.pactera.yhl.apps.develop.premiums.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResultWithColumnName;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaComuserMyTestLBCompute {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers",
                "10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group13");

        // 可选设置属性
        props.put("enable.auto.commit", "true");
        // 自动提交offset,每1s提交一次
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","latest ");
        props.put("client.id", "zy_client_id");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
//        consumer.subscribe(Collections.singletonList("APPLICATION_GENERAL_RESULT_RT"));
        consumer.subscribe(Collections.singletonList("testyhlv11LB"));
        Map<String,String> maps = new ConcurrentHashMap<String,String>();
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                String message = record.value();
                JSONObject jsonObject = JSON.parseObject(message);
                String signdate = jsonObject.get("signdate").toString();
                String modifydate = jsonObject.get("modifydate").toString();
                System.out.println("signdate = " + signdate +","+ "modifydate = "+modifydate);
            });
        }
    }
}
