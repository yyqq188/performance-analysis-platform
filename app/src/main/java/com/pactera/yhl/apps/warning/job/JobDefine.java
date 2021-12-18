package com.pactera.yhl.apps.warning.job;

import com.pactera.yhl.apps.warning.entity.AssessmentResult;
import com.pactera.yhl.apps.warning.entity.DirectorResult;
import com.pactera.yhl.apps.warning.entity.FactWageBase;
import com.pactera.yhl.apps.warning.entity.GeneralResult;
import com.pactera.yhl.apps.warning.map.MapFuncTableFactWageBase;
import com.pactera.yhl.apps.warning.sink.*;
import com.pactera.yhl.apps.warning.map.CalKhxzProcessFunction;
import com.pactera.yhl.apps.warning.map.CalKhyjProcessFunction;
import com.pactera.yhl.apps.warning.map.CalKhyjProcessFunctionTest;
import com.pactera.yhl.apps.warning.map.CalZjrsProcessFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author SUN KI
 * @time 2021/11/15 19:09
 * @Desc
 */
public class JobDefine {
    //一. 考核预警
    //1. 总考核人力+晋升 降级 维持计算
    public static void jobGeneralResult_Khrs(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<GeneralResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter(x -> "1".equals(x.getAgentstate()))
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhyjProcessFunction())
                .filter(x -> x != null);
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkGeneralKhzrsFunction());
    }

    //1. 总考核人力+晋升 降级 维持计算 测试用
    public static void jobGeneralResult_KhrsTest(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<GeneralResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter(x -> "1".equals(x.getAgentstate()))
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhyjProcessFunctionTest())
                .filter(x -> x != null);;
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkGeneralKhzrsFunction());
    }

    //2. 总监人力+晋升 降级 维持计算
    public static void jobGeneralResult_Zj(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<GeneralResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter(x -> "1".equals(x.getAgentstate()))
                .filter(new FilterFunction<FactWageBase>() {
                    @Override
                    public boolean filter(FactWageBase value) throws Exception {
                        List<String> list = new ArrayList<>();
                        list.add("A15");
                        list.add("A16");
                        list.add("A17");
                        list.add("A18");
                        list.add("A19");
                        return list.contains(value.getAgent_grade());
                    }
                })
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhyjProcessFunction())
                .filter(x -> x != null);;
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkGeneralZjFunction());
    }

    //3. 部经理人力+晋升 降级 维持计算
    public static void jobGeneralResult_Bjl(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<GeneralResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter(x -> "1".equals(x.getAgentstate()))
                .filter(new FilterFunction<FactWageBase>() {
                    @Override
                    public boolean filter(FactWageBase value) throws Exception {
                        List<String> list = new ArrayList<>();
                        list.add("A10");
                        list.add("A11");
                        list.add("A12");
                        list.add("A13");
                        list.add("A14");
                        return list.contains(value.getAgent_grade());
                    }
                })
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhyjProcessFunction())
                .filter(x -> x != null);;
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkGeneralBjlFunction());

    }

    //4. 专员人力+晋升 降级 维持计算
    public static void jobGeneralResult_Zy(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<GeneralResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter(x -> "1".equals(x.getAgentstate()))
                .filter(new FilterFunction<FactWageBase>() {
                    @Override
                    public boolean filter(FactWageBase value) throws Exception {
                        List<String> list = new ArrayList<>();
                        list.add("A01");
                        list.add("A02");
                        list.add("A03");
                        list.add("A04");
                        list.add("A05");
                        list.add("A06");
                        list.add("A07");
                        list.add("A08");
                        list.add("A09");
                        return list.contains(value.getAgent_grade());
                    }
                })
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhyjProcessFunction())
                .filter(x -> x != null);;
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkGeneralZyFunction());
    }

    //二. 考核预警总监级别分布
    public static void jobDirectorResult_Zj(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<DirectorResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter(x -> "1".equals(x.getAgentstate()))
                .filter(new FilterFunction<FactWageBase>() {
                    @Override
                    public boolean filter(FactWageBase value) throws Exception {
                        List<String> list = new ArrayList<>();
                        list.add("A15");
                        list.add("A16");
                        list.add("A17");
                        list.add("A18");
                        list.add("A19");
                        return list.contains(value.getAgent_grade());
                    }
                })
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalZjrsProcessFunction())
                .filter(x -> x != null);
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkDirectorFunction());
    }

    //三. 考核预警-基本法
    //全部人员
    public static void jobAssessmentResult_Qbry(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<AssessmentResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter(x -> "1".equals(x.getAgentstate()))
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhxzProcessFunction())
                .filter(x -> x != null);
        //sink到clickhouse
//        mapDS.print();
        mapDS.addSink(new clickhouseSinkSalaryKhzrsFunction());
    }

    //总监
    public static void jobAssessmentResult_Zj(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<AssessmentResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter((FilterFunction<FactWageBase>) value -> {
                    List<String> list = new ArrayList<>();
                    list.add("A15");
                    list.add("A16");
                    list.add("A17");
                    list.add("A18");
                    list.add("A19");
                    return "1".equals(value.getAgentstate()) && list.contains(value.getAgent_grade());
                })
                .filter(x -> "1".equals(x.getAgentstate()))
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhxzProcessFunction())
                .filter(x -> x != null);
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkSalaryZjFunction());
    }

    //部经理
    public static void jobAssessmentResult_Bjl(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<AssessmentResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter((FilterFunction<FactWageBase>) value -> {
                    List<String> list = new ArrayList<>();
                    list.add("A10");
                    list.add("A11");
                    list.add("A12");
                    list.add("A13");
                    list.add("A14");
                    return "1".equals(value.getAgentstate()) && list.contains(value.getAgent_grade());
                })
                .filter(x -> "1".equals(x.getAgentstate()))
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhxzProcessFunction())
                .filter(x -> x != null);
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkSalaryBjlFunction());
    }

    //专员
    public static void jobAssessmentResult_Zy(StreamExecutionEnvironment env, String topic, Properties properties, String groupId) {
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        DataStream<AssessmentResult> mapDS = source.map(new MapFuncTableFactWageBase())
                .filter(x -> x != null)
                .filter(new FilterFunction<FactWageBase>() {
                    @Override
                    public boolean filter(FactWageBase value) throws Exception {
                        List<String> list = new ArrayList<>();
                        list.add("A01");
                        list.add("A02");
                        list.add("A03");
                        list.add("A04");
                        list.add("A05");
                        list.add("A06");
                        list.add("A07");
                        list.add("A08");
                        list.add("A09");
                        return "1".equals(value.getAgentstate()) && list.contains(value.getAgent_grade());
                    }
                })
                .filter(x -> "1".equals(x.getAgentstate()))
                .keyBy(FactWageBase::getProvincecom_code)
                .map(new CalKhxzProcessFunction())
                .filter(x -> x != null);
        //sink到clickhouse
        mapDS.addSink(new clickhouseSinkSalaryZyFunction());
    }
}
