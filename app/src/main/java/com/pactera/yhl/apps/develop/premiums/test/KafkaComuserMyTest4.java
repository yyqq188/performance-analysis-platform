package com.pactera.yhl.apps.develop.premiums.test;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaComuserMyTest4 {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers",
                "10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group7");

        // 可选设置属性
        props.put("enable.auto.commit", "true");
        // 自动提交offset,每1s提交一次
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","latest ");
        props.put("client.id", "zy_client_id");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("testyhlv8LC"));
        AtomicLong num1 = new AtomicLong(0);
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
//                System.out.printf("topic = %s ,partition = %d,offset = %d, key = %s, value = %s%n", record.topic(), record.partition(),
//                        record.offset(), record.key(), record.value());

//                num1.addAndGet(1);
//                System.out.println("num = " + num1);
//                System.out.println(record.value());
////                PaserTest.ParseJson(record.value());

                Object branch_name = JSON.parseObject(record.value()).get("branch_name");
                Object contno = JSON.parseObject(record.value()).get("contno");
                Object payyears = JSON.parseObject(record.value()).get("payyears");
                System.out.println(branch_name +","+ contno+","+ payyears);
            });
        }

    }
}
