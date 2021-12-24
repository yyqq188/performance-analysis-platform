package com.pactera.yhl.apps.develop.premiums.main.job.lc;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity05;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductResultWithColumnName;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMapCount;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMapCountContno;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.ProductLCFlatMapCount;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnly;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnlyV2;
import com.pactera.yhl.constract.BranchMap;
import com.pactera.yhl.constract.ManageCom;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

public class AppProductResultLCV4 {
    //机构+产品
    public static void LC_branch_name_product (StreamExecutionEnvironment env,
                                               String inputTopic,
                                               Properties prop,
                                               String outputTopic,
                                               String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_branch_name_product");
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
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getBranch_name(),
                                premiumsKafkaEntity05.getProduct_name()
                        );
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductResultWithColumnName>() {
                    @Override
                    public ApplicationProductResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getBranch_name());
                        String manage_name = premiumsKafkaEntity05.getBranch_name();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String product_code = "";

                        if("".equals(premiumsKafkaEntity05.getContplancode()) || premiumsKafkaEntity05.getContplancode() == null){
                            product_code = premiumsKafkaEntity05.getRiskcode();
                        }else{
                            product_code = premiumsKafkaEntity05.getContplancode();
                        }
                        String product_name = premiumsKafkaEntity05.getProduct_name();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;
                        ApplicationProductResultWithColumnName applicationProductResult = new ApplicationProductResultWithColumnName();
                        applicationProductResult.setManage_name(manage_name);
                        applicationProductResult.setManage_code(manage_code);
                        applicationProductResult.setDay_id(day_id);
                        applicationProductResult.setKey_id(key_id);
                        applicationProductResult.setProduct_code(product_code);
                        applicationProductResult.setProduct_name(product_name);
                        applicationProductResult.setPrem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationProductResult.setColumnName("prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        return applicationProductResult;
                    }
                })
//                .addSink(new InsertKafkaOnly<>(outputTopic));
//                .addSink(new PremiumsClickhouseSink(tableName));
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductResultLC","APPLICATION_PRODUCT_RESULT_RT"));
    }
    //
    public static void LC_all_product (StreamExecutionEnvironment env,
                                               String inputTopic,
                                               Properties prop,
                                               String outputTopic,
                                               String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_all_product");
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
                        return Tuple2.of(premiumsKafkaEntity05.getSigndate(), "总公司");
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductResultWithColumnName>() {
                    @Override
                    public ApplicationProductResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = "86";
                        String manage_name = "总公司";
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String product_code = "";

                        if("".equals(premiumsKafkaEntity05.getContplancode()) || premiumsKafkaEntity05.getContplancode() == null){
                            product_code = premiumsKafkaEntity05.getRiskcode();
                        }else{
                            product_code = premiumsKafkaEntity05.getContplancode();
                        }
                        String product_name = premiumsKafkaEntity05.getProduct_name();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;
                        ApplicationProductResultWithColumnName applicationProductResult = new ApplicationProductResultWithColumnName();
                        applicationProductResult.setManage_name(manage_name);
                        applicationProductResult.setManage_code(manage_code);
                        applicationProductResult.setDay_id(day_id);
                        applicationProductResult.setKey_id(key_id);
                        applicationProductResult.setProduct_code(product_code);
                        applicationProductResult.setProduct_name(product_name);
                        applicationProductResult.setPrem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationProductResult.setColumnName("prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        return applicationProductResult;
                    }
                })
//                .addSink(new InsertKafkaOnly<>(outputTopic));
//                .addSink(new PremiumsClickhouseSink(tableName));
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductResultLC","APPLICATION_PRODUCT_RESULT_RT"));
    }


    public static void LC_branch_name_product_num (StreamExecutionEnvironment env,
                                                   String inputTopic,
                                                   Properties prop,
                                                   String outputTopic,
                                                   String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_branch_name_product_num");
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
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getBranch_name(),
                                premiumsKafkaEntity05.getProduct_name()
                        );
                    }})
                .flatMap(new OrganizationLCFlatMapCountContno())
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductResultWithColumnName>() {
                    @Override
                    public ApplicationProductResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getBranch_name());
                        String manage_name = premiumsKafkaEntity05.getBranch_name();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String product_code = "";

                        if("".equals(premiumsKafkaEntity05.getContplancode()) || premiumsKafkaEntity05.getContplancode() == null){
                            product_code = premiumsKafkaEntity05.getRiskcode();
                        }else{
                            product_code = premiumsKafkaEntity05.getContplancode();
                        }
                        String product_name = premiumsKafkaEntity05.getProduct_name();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductResultWithColumnName applicationProductResult = new ApplicationProductResultWithColumnName();
                        applicationProductResult.setManage_name(manage_name);
                        applicationProductResult.setManage_code(manage_code);
                        applicationProductResult.setDay_id(day_id);
                        applicationProductResult.setKey_id(key_id);
                        applicationProductResult.setProduct_code(product_code);
                        applicationProductResult.setProduct_name(product_name);
                        applicationProductResult.setNum_day(Double.valueOf(premiumsKafkaEntity05.getContno()));
                        applicationProductResult.setColumnName("num_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        return applicationProductResult;
                    }
                })
//                .addSink(new InsertKafkaOnly<>(outputTopic));
//                .addSink(new PremiumsClickhouseSink(tableName));
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductResultLC","APPLICATION_PRODUCT_RESULT_RT"));

    }
    //LC_all_product_num
    public static void LC_all_product_num (StreamExecutionEnvironment env,
                                                   String inputTopic,
                                                   Properties prop,
                                                   String outputTopic,
                                                   String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_all_product_num");
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
                                "总公司");
                    }})
                .flatMap(new OrganizationLCFlatMapCountContno())
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductResultWithColumnName>() {
                    @Override
                    public ApplicationProductResultWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = "86";
                        String manage_name = "总公司";
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }                        String product_code = "";

                        if("".equals(premiumsKafkaEntity05.getContplancode()) || premiumsKafkaEntity05.getContplancode() == null){
                            product_code = premiumsKafkaEntity05.getRiskcode();
                        }else{
                            product_code = premiumsKafkaEntity05.getContplancode();
                        }
                        String product_name = premiumsKafkaEntity05.getProduct_name();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductResultWithColumnName applicationProductResult = new ApplicationProductResultWithColumnName();
                        applicationProductResult.setManage_name(manage_name);
                        applicationProductResult.setManage_code(manage_code);
                        applicationProductResult.setDay_id(day_id);
                        applicationProductResult.setKey_id(key_id);
                        applicationProductResult.setProduct_code(product_code);
                        applicationProductResult.setProduct_name(product_name);
                        applicationProductResult.setNum_day(Double.valueOf(premiumsKafkaEntity05.getContno()));
                        applicationProductResult.setColumnName("num_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        return applicationProductResult;
                    }
                })
//                .addSink(new InsertKafkaOnly<>(outputTopic));
//                .addSink(new PremiumsClickhouseSink(tableName));
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductResultLC","APPLICATION_PRODUCT_RESULT_RT"));

    }

}
