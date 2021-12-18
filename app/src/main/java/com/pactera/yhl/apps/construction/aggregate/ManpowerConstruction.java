package com.pactera.yhl.apps.construction.aggregate;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.construction.entity.PremiumsPOJO;
import com.pactera.yhl.apps.construction.statejob.MergeCountStateMap;
import com.pactera.yhl.apps.construction.statejob.TotalMergeCountStateMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author: TSY
 * @create: 2021/11/15 0015 上午 10:37
 * @description:  日入职人力：
 *
 * rankid
 * 日入职总监人力    A15 A16 A17 A18 A19
 * 日入职部经理人力  A10 A11 A12 A13 A14
 * 日入职专员人力    A01 A02 A03 A04 A05 A06 A07 A08 A09
 * 日入职人力
 */
public class ManpowerConstruction {

    public static void construction(StreamExecutionEnvironment env, String topic, Properties kafkaprops) {

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaprops);

        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
//        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        //将json转换成实体类对象
        SingleOutputStreamOperator<PremiumsPOJO> source = env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsPOJO>() {
                    @Override
                    public PremiumsPOJO map(String value) throws Exception {
                        return JSONObject.parseObject(value, PremiumsPOJO.class);
                    }
                });


        //分公司
        source.map(new MapFunction<PremiumsPOJO, Tuple2<String, PremiumsPOJO>>() {
            @Override
            public Tuple2<String, PremiumsPOJO> map(PremiumsPOJO value) throws Exception {
                return Tuple2.of(value.getKey_id(), value);
            }
        }).keyBy(x -> x.f0).map(new MergeCountStateMap()).print("分公司");

        //总公司
        source.map(new MapFunction<PremiumsPOJO, Tuple2<String, PremiumsPOJO>>() {
            @Override
            public Tuple2<String, PremiumsPOJO> map(PremiumsPOJO value) throws Exception {
                return Tuple2.of(value.getKey_id().substring(0, 10) + "#86", value);
            }
        }).keyBy(x -> x.f0).map(new TotalMergeCountStateMap()).print("总公司");

    }
}
