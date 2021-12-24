package com.pactera.yhl.apps.develop.premiums.main.job.lc;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity05;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductDetialWithColumnName;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMapCount;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMapCountContno;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnlyV2;
import com.pactera.yhl.constract.BranchMap;
import com.pactera.yhl.constract.ManageCom;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;



public class AppProductDetailLCV4 {

    public static void LC_branch_name_product (StreamExecutionEnvironment env,
                                                         String inputTopic,
                                                         Properties prop,
                                                         String outputTopic,
                                                         String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_branch_name_product");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
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
                        premiumsKafkaEntity05.setBranch_id("86" + premiumsKafkaEntity05.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getBranch_name(),
                                premiumsKafkaEntity05.getProduct_name());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductDetialWithColumnName>() {
                    @Override
                    public ApplicationProductDetialWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
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
                        String pay_period = premiumsKafkaEntity05.getPay_period();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
                        applicationProductDetial.setManage_name(manage_name);
                        applicationProductDetial.setManage_code(manage_code);
                        applicationProductDetial.setDay_id(day_id);
                        applicationProductDetial.setKey_id(key_id);
                        applicationProductDetial.setProduct_code(product_code);
                        applicationProductDetial.setProduct_name(product_name);
                        applicationProductDetial.setPrem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        applicationProductDetial.setColumnName("prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");

                        return applicationProductDetial;
                    }
                })
                .filter(Objects::nonNull)
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductDetailLC","APPLICATION_PRODUCT_DETIAL_RT"));
    }





    public static void LC_branch_name_product_payperiod (StreamExecutionEnvironment env,
                                                         String inputTopic,
                                                         Properties prop,
                                                         String outputTopic,
                                                         String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_branch_name_product_payperiod");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
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
                        premiumsKafkaEntity05.setBranch_id("86" + premiumsKafkaEntity05.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple4.of(premiumsKafkaEntity05.getSigndate(), premiumsKafkaEntity05.getBranch_name(),
                                premiumsKafkaEntity05.getProduct_name(),premiumsKafkaEntity05.getPay_period());
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductDetialWithColumnName>() {
                    @Override
                    public ApplicationProductDetialWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity05.getBranch_name());
                        String manage_name = premiumsKafkaEntity05.getBranch_name();
                        String day_id = "";
                        if(!StringUtils.isBlank(premiumsKafkaEntity05.getSigndate())){
                            day_id = premiumsKafkaEntity05.getSigndate().split("\\s+")[0];
                        }
                        String product_code = "";

                        if("".equals(premiumsKafkaEntity05.getContplancode()) || premiumsKafkaEntity05.getContplancode() == null){
                            product_code = premiumsKafkaEntity05.getRiskcode();
                        }else{
                            product_code = premiumsKafkaEntity05.getContplancode();
                        }
                        String product_name = premiumsKafkaEntity05.getProduct_name();
                        String pay_period = premiumsKafkaEntity05.getPayyears();  //这里有变化
                        String key_id = day_id +"#" +manage_code +"#" + product_code;
                        System.out.println("manage_name = " + manage_name);
                        System.out.println("manage_code = " + manage_code);
                        System.out.println("day_id = " + day_id);
                        System.out.println("key_id = " + key_id);
                        System.out.println("product_code = " + product_code);
                        System.out.println("product_name = " + product_name);

                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
                        applicationProductDetial.setManage_name(manage_name);
                        applicationProductDetial.setManage_code(manage_code);
                        applicationProductDetial.setDay_id(day_id);
                        applicationProductDetial.setKey_id(key_id);
                        applicationProductDetial.setProduct_code(product_code);
                        applicationProductDetial.setProduct_name(product_name);
                        applicationProductDetial.setPrem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));

                        if("0".equals(pay_period)){
                            applicationProductDetial.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("single_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }
                        else if("3".equals(pay_period)){
                            applicationProductDetial.setThree_year_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("three_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }
                        else if("5".equals(pay_period)){
                            applicationProductDetial.setFive_year_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("five_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }
                        else if("10".equals(pay_period)){
                            applicationProductDetial.setTen_year_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("ten_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("20".equals(pay_period)){
                            applicationProductDetial.setTwenty_year_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("twenty_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else{
                            return null;
                        }
                        return applicationProductDetial;
                    }
                })
                .filter(Objects::nonNull)
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductDetailLC","APPLICATION_PRODUCT_DETIAL_RT"));
    }
    //
    public static void LC_all_product_payperiod (StreamExecutionEnvironment env,
                                                         String inputTopic,
                                                         Properties prop,
                                                         String outputTopic,
                                                         String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_all_product_payperiod");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
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
                        premiumsKafkaEntity05.setBranch_id("86" + premiumsKafkaEntity05.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple4.of(premiumsKafkaEntity05.getSigndate(), "总公司",
                                premiumsKafkaEntity05.getProduct_name(),premiumsKafkaEntity05.getPay_period());
                    }})
                .flatMap(new OrganizationLCFlatMap())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductDetialWithColumnName>() {
                    @Override
                    public ApplicationProductDetialWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
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
                        String pay_period = premiumsKafkaEntity05.getPay_period();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
                        applicationProductDetial.setManage_name(manage_name);
                        applicationProductDetial.setManage_code(manage_code);
                        applicationProductDetial.setDay_id(day_id);
                        applicationProductDetial.setKey_id(key_id);
                        applicationProductDetial.setProduct_code(product_code);
                        applicationProductDetial.setProduct_name(product_name);
                        applicationProductDetial.setPrem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                        if("0".equals(pay_period)){
                            applicationProductDetial.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("single_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("3".equals(pay_period)){
                            applicationProductDetial.setThree_year_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("three_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("5".equals(pay_period)){
                            applicationProductDetial.setFive_year_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("five_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("10".equals(pay_period)){
                            applicationProductDetial.setTen_year_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("ten_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("20".equals(pay_period)){
                            applicationProductDetial.setTwenty_year_prem_day(Double.valueOf(premiumsKafkaEntity05.getPrem()));
                            applicationProductDetial.setColumnName("twenty_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else{
                            return null;
                        }
                        return applicationProductDetial;
                    }
                })
                .filter(Objects::nonNull)
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductDetailLC","APPLICATION_PRODUCT_DETIAL_RT"));
    }
    public static void LC_branch_name_product_payperiod_num (StreamExecutionEnvironment env,
                                                             String inputTopic,
                                                             Properties prop,
                                                             String outputTopic,
                                                             String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_branch_name_product_payperiod_num");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
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
                        premiumsKafkaEntity05.setBranch_id("86" + premiumsKafkaEntity05.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple4.of(premiumsKafkaEntity05.getSigndate(),
                                premiumsKafkaEntity05.getBranch_name(),
                                premiumsKafkaEntity05.getProduct_name(),
                                premiumsKafkaEntity05.getPayyears());
                    }})
                .flatMap(new OrganizationLCFlatMapCountContno())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductDetialWithColumnName>() {
                    @Override
                    public ApplicationProductDetialWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
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
                        String pay_period = premiumsKafkaEntity05.getPayyears();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
                        applicationProductDetial.setManage_name(manage_name);
                        applicationProductDetial.setManage_code(manage_code);
                        applicationProductDetial.setDay_id(day_id);
                        applicationProductDetial.setKey_id(key_id);
                        applicationProductDetial.setProduct_code(product_code);
                        applicationProductDetial.setProduct_name(product_name);

                        applicationProductDetial.setNum_day(Double.valueOf(premiumsKafkaEntity05.getContno()));
                        applicationProductDetial.setColumnName("num_day;key_id,day_id,manage_code,manage_name,product_code,product_name");

                        return applicationProductDetial;
                    }
                })
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductDetailLC","APPLICATION_PRODUCT_DETIAL_RT"));
    }
    //
    public static void LC_all_product_payperiod_num (StreamExecutionEnvironment env,
                                                             String inputTopic,
                                                             Properties prop,
                                                             String outputTopic,
                                                             String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LC_all_product_payperiod_num");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
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
                        premiumsKafkaEntity05.setBranch_id("86" + premiumsKafkaEntity05.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity05;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity05, Tuple4<String,String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> getKey(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
                        return Tuple4.of(premiumsKafkaEntity05.getSigndate(),
                                "总公司",
                                premiumsKafkaEntity05.getProduct_name(),
                                premiumsKafkaEntity05.getPay_period());
                    }})
                .flatMap(new OrganizationLCFlatMapCountContno())

                .map(new MapFunction<PremiumsKafkaEntity05, ApplicationProductDetialWithColumnName>() {
                    @Override
                    public ApplicationProductDetialWithColumnName map(PremiumsKafkaEntity05 premiumsKafkaEntity05) throws Exception {
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
                        String pay_period = premiumsKafkaEntity05.getPay_period();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
                        applicationProductDetial.setManage_name(manage_name);
                        applicationProductDetial.setManage_code(manage_code);
                        applicationProductDetial.setDay_id(day_id);
                        applicationProductDetial.setKey_id(key_id);
                        applicationProductDetial.setProduct_code(product_code);
                        applicationProductDetial.setProduct_name(product_name);

                        applicationProductDetial.setNum_day(Double.valueOf(premiumsKafkaEntity05.getContno()));
                        applicationProductDetial.setColumnName("num_day;key_id,day_id,manage_code,manage_name,product_code,product_name");

                        return applicationProductDetial;
                    }
                })
                .addSink(new InsertHbaseOnlyV2<>("KLMIDAPPRUN:AppProductDetailLC","APPLICATION_PRODUCT_DETIAL_RT"));
    }
}
