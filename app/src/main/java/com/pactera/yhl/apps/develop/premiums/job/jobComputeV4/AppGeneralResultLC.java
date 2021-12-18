package com.pactera.yhl.apps.develop.premiums.job.jobComputeV4;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResultWithColumnName;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResultWithFieldColumnName;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnly;
import com.pactera.yhl.apps.develop.premiums.sink.PremiumsClickhouseSink;
import com.pactera.yhl.constract.BranchMap;
import com.pactera.yhl.constract.ManageCom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

public class AppGeneralResultLC {
//    public static void LC_branch_name (StreamExecutionEnvironment env,
//                                       String inputTopic,
//                                       Properties prop,
//                                       String outputTopic,
//                                       String tableName){
//        Map<String, String> manageCom = ManageCom.manageCom;
//        Map<String, String> branchMap = BranchMap.map;
//        prop.setProperty("group.id","LC_branch_name");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                inputTopic,
//                new SimpleStringSchema(),
//                prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        SingleOutputStreamOperator<ApplicationGeneralResultWithColumnName> source = env.addSource(kafkaConsumer)
//                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(String s) throws Exception {
//                        return JSON.parseObject(s, PremiumsKafkaEntity04.class);
//                    }
//                })
//                .filter(x -> x.signdate.length() > 0)
//                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        if(branchMap.keySet().contains(premiumsKafkaEntity04.getBranch_name())){
//                            premiumsKafkaEntity04.setBranch_name(branchMap.get(premiumsKafkaEntity04.getBranch_name()));
//                        }
//                        return premiumsKafkaEntity04;
//                    }
//                })
//                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        return Tuple2.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name());
//                    }
//                })
//                .flatMap(new OrganizationLCFlatMap())
//
//                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationGeneralResultWithColumnName>() {
//                    @Override
//                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        String manage_code = manageCom.get(premiumsKafkaEntity04.getBranch_name());
//                        String manage_name = premiumsKafkaEntity04.getBranch_name();
//                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
//                        String period_type = premiumsKafkaEntity04.getPeriod_type();
//                        String key_id = day_id + "#" + manage_code;
//                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
//                        applicationGeneralResult.setManage_name(manage_name);
//                        applicationGeneralResult.setManage_code(manage_code);
//                        applicationGeneralResult.setDay_id(day_id);
//                        applicationGeneralResult.setKey_id(key_id);
//                        applicationGeneralResult.setPrem_new_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                        applicationGeneralResult.setColumnName("AppGeneralResultLC");
//                        //正对decimal类型得处理，不能是null需要专门给0.0
//                        return applicationGeneralResult;
//                    }
//                });
//        source.addSink(new InsertHbaseOnly<>("KLMIDAPPRUN:AppGeneralResultLC"));
//    }

//    public static void LC_workarea (StreamExecutionEnvironment env,
//                                    String inputTopic,
//                                    Properties prop,
//                                    String outputTopic,
//                                    String tableName){
//        Map<String, String> manageCom = ManageCom.manageCom;
//        Map<String, String> branchMap = BranchMap.map;
//        prop.setProperty("group.id","LC_workarea");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                inputTopic,
//                new SimpleStringSchema(),
//                prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        SingleOutputStreamOperator<ApplicationGeneralResultWithFieldColumnName> source = env.addSource(kafkaConsumer)
//                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(String s) throws Exception {
//                        return JSON.parseObject(s, PremiumsKafkaEntity04.class);
//                    }
//                })
//                .filter(x -> x.signdate.length() > 0)
//                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        if(branchMap.keySet().contains(premiumsKafkaEntity04.getBranch_name())){
//                            premiumsKafkaEntity04.setBranch_name(branchMap.get(premiumsKafkaEntity04.getBranch_name()));
//                        }
//                        return premiumsKafkaEntity04;
//                    }
//                })
//                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        return Tuple2.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getWorkarea());
//                    }
//                })
//                .flatMap(new OrganizationLCFlatMap())
//
//                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationGeneralResultWithFieldColumnName>() {
//                    @Override
//                    public ApplicationGeneralResultWithFieldColumnName map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        String manage_code = manageCom.get(premiumsKafkaEntity04.getWorkarea());
//                        String manage_name = premiumsKafkaEntity04.getWorkarea();
//                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
//                        String key_id = day_id + "#" + manage_code;
//                        ApplicationGeneralResultWithFieldColumnName applicationGeneralResult = new ApplicationGeneralResultWithFieldColumnName();
//                        applicationGeneralResult.setManage_name(manage_name);
//                        applicationGeneralResult.setManage_code(manage_code);
//                        applicationGeneralResult.setDay_id(day_id);
//                        applicationGeneralResult.setKey_id(key_id);
//                        applicationGeneralResult.setPrem_new_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                        applicationGeneralResult.setFieldName("key_id,day_id,manage_code,manage_name,prem_new_day");
//                        applicationGeneralResult.setColumnName("AppGeneralResultLC");
//                        return applicationGeneralResult;
//                    }
//                });
//
//        source.addSink(new PremiumsClickhouseSink(tableName));
//        source.addSink(new InsertHbaseOnly<>("KLMIDAPPRUN:AppGeneralResultLC"));
//    }

}
