package com.pactera.yhl.apps.develop.premiums.job.jobComputeV3;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResult;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.sink.InsertKafkaOnly;
import com.pactera.yhl.apps.develop.premiums.sink.PremiumsClickhouseSink;
import com.pactera.yhl.constract.ManageCom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

public class AppGeneralResultLC {
    //todo 浙江分公司
    public static void LC_branch_name (StreamExecutionEnvironment env,
                                       String inputTopic,
                                       Properties prop,
                                       String outputTopic,
                                       String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        prop.setProperty("group.id","LC_branch_name");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationGeneralResult>() {
                    @Override
                    public ApplicationGeneralResult map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity04.getBranch_name());
                        String manage_name = premiumsKafkaEntity04.getBranch_name();
                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
                        String period_type = premiumsKafkaEntity04.getPeriod_type();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResult applicationGeneralResult = new ApplicationGeneralResult();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);

                        applicationGeneralResult.setPrem_new_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));

                        //正对decimal类型得处理，不能是null需要专门给0.0
                        return applicationGeneralResult;
                    }
                })
//                .addSink(new InsertKafkaOnly<>(outputTopic));
                .addSink(new PremiumsClickhouseSink(tableName));
    }

    //todo 宁波
    public static void LC_workarea (StreamExecutionEnvironment env,
                                    String inputTopic,
                                    Properties prop,
                                    String outputTopic,
                                    String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        prop.setProperty("group.id","LC_workarea");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getWorkarea());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationGeneralResult>() {
                    @Override
                    public ApplicationGeneralResult map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity04.getWorkarea());
                        String manage_name = premiumsKafkaEntity04.getWorkarea();
                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResult applicationGeneralResult = new ApplicationGeneralResult();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setPrem_new_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));

                        return applicationGeneralResult;
                    }
                })

//                .addSink(new InsertKafkaOnly<>(outputTopic));
                .addSink(new PremiumsClickhouseSink(tableName));
    }

    //todo  浙江分公司 期趸交
    public static void LC_branch_name_periodtype (StreamExecutionEnvironment env,
                                       String inputTopic,
                                       Properties prop,
                                       String outputTopic,
                                       String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        prop.setProperty("group.id","LC_branch_name");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
                                premiumsKafkaEntity04.getPeriod_type());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationGeneralResult>() {
                    @Override
                    public ApplicationGeneralResult map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity04.getBranch_name());
                        String manage_name = premiumsKafkaEntity04.getBranch_name();
                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
                        String period_type = premiumsKafkaEntity04.getPeriod_type();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResult applicationGeneralResult = new ApplicationGeneralResult();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        if("期交".equals(period_type.trim())){
                            applicationGeneralResult.setRegular_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
                        }else if("趸交".equals(period_type.trim())){
                            applicationGeneralResult.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
                        }
//                        applicationGeneralResult.setPrem_new_day(premiumsKafkaEntity04.getPrem());

                        return applicationGeneralResult;
                    }
                })

//                .addSink(new InsertKafkaOnly<>(outputTopic));
                .addSink(new PremiumsClickhouseSink(tableName));
    }
    //todo 宁波 期趸交
    public static void LC_workarea_periodtype (StreamExecutionEnvironment env,
                                       String inputTopic,
                                       Properties prop,
                                       String outputTopic,
                                       String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        prop.setProperty("group.id","LC_workarea");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getWorkarea(),
                                premiumsKafkaEntity04.getPeriod_type());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationGeneralResult>() {
                    @Override
                    public ApplicationGeneralResult map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity04.getWorkarea());
                        String manage_name = premiumsKafkaEntity04.getWorkarea();
                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
                        String period_type = premiumsKafkaEntity04.getPeriod_type();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResult applicationGeneralResult = new ApplicationGeneralResult();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        if("期交".equals(period_type.trim())){
                            applicationGeneralResult.setRegular_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
                        }else if("趸交".equals(period_type.trim())){
                            applicationGeneralResult.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
                        }

                        return applicationGeneralResult;
                    }
                })
//                .addSink(new InsertKafkaOnly<>(outputTopic));
                .addSink(new PremiumsClickhouseSink(tableName));

    }

}
