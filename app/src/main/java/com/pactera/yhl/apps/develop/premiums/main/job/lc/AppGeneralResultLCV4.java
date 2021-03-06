package com.pactera.yhl.apps.develop.premiums.main.job.lc;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity05;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResultWithColumnName;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResultWithFieldColumnName;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMapCount;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMapCountContno;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnly;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnlyV2;
import com.pactera.yhl.apps.develop.premiums.sink.PremiumsClickhouseSink;
import com.pactera.yhl.constract.BranchMap;
import com.pactera.yhl.constract.ManageCom;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

public class AppGeneralResultLCV4 {
    public static void LC_branch_name (StreamExecutionEnvironment env,
                                       String inputTopic,
                                       Properties prop,
                                       String outputTopic,
                                       String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_branch_name");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        SingleOutputStreamOperator<ApplicationGeneralResultWithColumnName> source = env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s, PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    // put("???????????????????????????","???????????????");
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getBranch_name());
                    }
                })
                .flatMap(new OrganizationLCFlatMap())
                //put("??????","863303");
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getBranch_name());
                        String manage_name = premiumsKafkaEntity05.getBranch_name();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }
                        String period_type = premiumsKafkaEntity05.getProduct_payintv();
                        String key_id = day_id + "#" + manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setPrem_new_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationGeneralResult.setPrem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationGeneralResult.setApprove_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        //??????;rowkey    rowkey??????,??????????????????
                        applicationGeneralResult.setColumnName("prem_day,prem_new_day,approve_prem_day;key_id,day_id,manage_code,manage_name");
                        return applicationGeneralResult;
                    }
                });
        source.addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }

    public static void LC_workarea (StreamExecutionEnvironment env,
                                    String inputTopic,
                                    Properties prop,
                                    String outputTopic,
                                    String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_workarea");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        SingleOutputStreamOperator<ApplicationGeneralResultWithColumnName> source = env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s, PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    // put("???????????????????????????","???????????????");
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getWorkarea());
                    }
                })
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        //put("??????","863303");
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getWorkarea());
                        String manage_name = premiumsKafkaEntity05.getWorkarea();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String key_id = day_id + "#" + manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setPrem_new_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationGeneralResult.setPrem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationGeneralResult.setApprove_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationGeneralResult.setColumnName("prem_day,prem_new_day,approve_prem_day;key_id,day_id,manage_code,manage_name");
                        return applicationGeneralResult;
                    }
                });
        source.addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }



    public static void LC_all (StreamExecutionEnvironment env,
                                    String inputTopic,
                                    Properties prop,
                                    String outputTopic,
                                    String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_all");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        SingleOutputStreamOperator<ApplicationGeneralResultWithColumnName> source = env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s, PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity05.getSigndate(), "?????????");
                    }
                })
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = "86";
                        String manage_name = "?????????";
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String key_id = day_id + "#" + manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setPrem_new_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationGeneralResult.setPrem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationGeneralResult.setApprove_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationGeneralResult.setColumnName(
                                "prem_day,prem_new_day,approve_prem_day;key_id,day_id,manage_code,manage_name"
                        );
                        return applicationGeneralResult;
                    }
                });
        source.addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }

    public static void LC_branch_name_periodtype (StreamExecutionEnvironment env,
                                                  String inputTopic,
                                                  Properties prop,
                                                  String outputTopic,
                                                  String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_branch_name");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getBranch_name(),
                                premiumsKafkaEntity05.getProduct_payintv());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getBranch_name());
                        String manage_name = premiumsKafkaEntity05.getBranch_name();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String period_type = premiumsKafkaEntity05.getProduct_payintv();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        if("??????".equals(period_type.trim())){
                            applicationGeneralResult.setRegular_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationGeneralResult.setColumnName("regular_prem_day;key_id,day_id,manage_code,manage_name");
                        }else if("??????".equals(period_type.trim())){
                            applicationGeneralResult.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationGeneralResult.setColumnName("single_prem_day;key_id,day_id,manage_code,manage_name");
                        }
                        return applicationGeneralResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }
    //todo ?????? ?????????
    public static void LC_workarea_periodtype (StreamExecutionEnvironment env,
                                               String inputTopic,
                                               Properties prop,
                                               String outputTopic,
                                               String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_workarea");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity05.getSigndate(), premiumsKafkaEntity05.getWorkarea(),
                                premiumsKafkaEntity05.getProduct_payintv());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getWorkarea());
                        String manage_name = premiumsKafkaEntity05.getWorkarea();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String period_type = premiumsKafkaEntity05.getProduct_payintv();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        if("??????".equals(period_type.trim())){
                            applicationGeneralResult.setRegular_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationGeneralResult.setColumnName("regular_prem_day;key_id,day_id,manage_code,manage_name");
                        }else if("??????".equals(period_type.trim())){
                            applicationGeneralResult.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationGeneralResult.setColumnName("single_prem_day;key_id,day_id,manage_code,manage_name");
                        }
                        return applicationGeneralResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));

    }
    //?????????
    public static void LC_all_periodtype (StreamExecutionEnvironment env,
                                               String inputTopic,
                                               Properties prop,
                                               String outputTopic,
                                               String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_all_periodtype");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity05.getSigndate(),"?????????",
                                premiumsKafkaEntity05.getProduct_payintv());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = "86";
                        String manage_name = "?????????";
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String period_type = premiumsKafkaEntity05.getProduct_payintv();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        if("??????".equals(period_type.trim())){
                            applicationGeneralResult.setRegular_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationGeneralResult.setColumnName("regular_prem_day;key_id,day_id,manage_code,manage_name");
                        }else if("??????".equals(period_type.trim())){
                            applicationGeneralResult.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationGeneralResult.setColumnName("single_prem_day;key_id,day_id,manage_code,manage_name");
                        }
                        return applicationGeneralResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));

    }





    //todo ???????????????  ????????????
    public static void LC_branch_name_num (StreamExecutionEnvironment env,
                                           String inputTopic,
                                           Properties prop,
                                           String outputTopic,
                                           String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_branch_name");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getBranch_name());
                    }})
                .flatMap(new OrganizationLCFlatMapCountContno())
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getBranch_name());
                        String manage_name = premiumsKafkaEntity05.getBranch_name();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String period_type = premiumsKafkaEntity05.getProduct_payintv();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setApprove_num_day(Double.valueOf(premiumsKafkaEntity05.getContno()));
                        applicationGeneralResult.setColumnName("approve_num_day;key_id,day_id,manage_code,manage_name");
                        return applicationGeneralResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }


    //todo ??????  ????????????
    public static void LC_workarea_num (StreamExecutionEnvironment env,
                                        String inputTopic,
                                        Properties prop,
                                        String outputTopic,
                                        String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_workarea");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getBranch_name());
                    }})
                .flatMap(new OrganizationLCFlatMapCountContno())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getWorkarea());
                        String manage_name = premiumsKafkaEntity05.getWorkarea();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String period_type = premiumsKafkaEntity05.getProduct_payintv();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setApprove_num_day(Double.valueOf(premiumsKafkaEntity05.getContno()));
                        applicationGeneralResult.setColumnName("approve_num_day;key_id,day_id,manage_code,manage_name");

                        return applicationGeneralResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }
    //
    public static void LC_all_num (StreamExecutionEnvironment env,
                                        String inputTopic,
                                        Properties prop,
                                        String outputTopic,
                                        String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_all_num");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<PremiumsKafkaEntity05, PremiumsKafkaEntity05>() {
                    @Override
                    public PremiumsKafkaEntity05 map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        if(branchMap.keySet().contains(premiumsKafkaEntity05.getBranch_name())){
                            premiumsKafkaEntity05.setBranch_name(branchMap.get(premiumsKafkaEntity05.getBranch_name()));
                        }
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity05.getSigndate(), "?????????");
                    }})
                .flatMap(new OrganizationLCFlatMapCountContno())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = "86";
                        String manage_name = "?????????";
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }
                        String period_type = premiumsKafkaEntity05.getProduct_payintv();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setApprove_num_day(Double.valueOf(premiumsKafkaEntity05.getContno()));
                        applicationGeneralResult.setColumnName("approve_num_day;key_id,day_id,manage_code,manage_name");

                        return applicationGeneralResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }
}
