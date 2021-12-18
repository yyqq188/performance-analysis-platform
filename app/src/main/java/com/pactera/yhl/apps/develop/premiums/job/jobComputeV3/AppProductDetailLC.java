package com.pactera.yhl.apps.develop.premiums.job.jobComputeV3;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductDetial;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductDetialWithColumnName;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductDetialWithFieldName;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductResult;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMapCount;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnly;
import com.pactera.yhl.apps.develop.premiums.sink.InsertKafkaOnly;
import com.pactera.yhl.apps.develop.premiums.sink.PremiumsClickhouseSink;
import com.pactera.yhl.constract.BranchMap;
import com.pactera.yhl.constract.ManageCom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

public class AppProductDetailLC {
//    //todo 浙江 产品 年期
//    public static void LC_branch_name_product_payperiod (StreamExecutionEnvironment env,
//                                                         String inputTopic,
//                                                         Properties prop,
//                                                         String outputTopic,
//                                                         String tableName){
//        Map<String, String> manageCom = ManageCom.manageCom;
//        Map<String, String> branchMap = BranchMap.map;
//        prop.setProperty("group.id","LC_branch_name_product_payperiod");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                inputTopic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        env.addSource(kafkaConsumer)
//                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(String s) throws Exception {
//                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
//                    }
//                })
//                .filter(x -> x.signdate.length() > 0)
//                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        if(branchMap.keySet().contains(premiumsKafkaEntity04.getBranch_name())){
//                            premiumsKafkaEntity04.setBranch_name(branchMap.get(premiumsKafkaEntity04.getBranch_name()));
//                        }
//                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
//                        return premiumsKafkaEntity04;
//                    }
//                })
//                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple4<String, String,String,String>>() {
//                    @Override
//                    public Tuple4<String, String,String,String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        return Tuple4.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
//                                premiumsKafkaEntity04.getProduct_name(),premiumsKafkaEntity04.getPay_period());
//                    }})
//                .flatMap(new OrganizationLCFlatMap())
//
//                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationProductDetialWithColumnName>() {
//                    @Override
//                    public ApplicationProductDetialWithColumnName map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        String manage_code = manageCom.get(premiumsKafkaEntity04.getBranch_name());
//                        String manage_name = premiumsKafkaEntity04.getBranch_name();
//                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
//                        String product_code = "";
//
//                        if("".equals(premiumsKafkaEntity04.getContplancode()) || premiumsKafkaEntity04.getContplancode() == null){
//                            product_code = premiumsKafkaEntity04.getRiskcode();
//                        }else{
//                            product_code = premiumsKafkaEntity04.getContplancode();
//                        }
//                        String product_name = premiumsKafkaEntity04.getProduct_name();
//                        String pay_period = premiumsKafkaEntity04.getPay_period();
//                        String key_id = day_id +"#" +manage_code +"#" + product_code;
//
//                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
//                        applicationProductDetial.setManage_name(manage_name);
//                        applicationProductDetial.setManage_code(manage_code);
//                        applicationProductDetial.setDay_id(day_id);
//                        applicationProductDetial.setKey_id(key_id);
//                        applicationProductDetial.setProduct_code(product_code);
//                        applicationProductDetial.setProduct_name(product_name);
//                        applicationProductDetial.setPrem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                        if("0".equals(pay_period)){
//                            applicationProductDetial.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("0");
//                        }else if("3".equals(pay_period)){
//                            applicationProductDetial.setThree_year_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("3");
//                        }else if("5".equals(pay_period)){
//                            applicationProductDetial.setFive_year_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("5");
//                        }else if("10".equals(pay_period)){
//                            applicationProductDetial.setTen_year_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("10");
//                        }else if("20".equals(pay_period)){
//                            applicationProductDetial.setTwenty_year_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("20");
//                        }
//                        return applicationProductDetial;
//                    }
//                })
//                .addSink(new InsertHbaseOnly<>("KLMIDAPPRUN:AppProductDetailLC"));
//    }
//
//
//
//
//    //todo 浙江 产品 年期
//    public static void LC_branch_name_product_payperiod_approve (StreamExecutionEnvironment env,
//                                                         String inputTopic,
//                                                         Properties prop,
//                                                         String outputTopic,
//                                                         String tableName){
//        Map<String, String> manageCom = ManageCom.manageCom;
//        Map<String, String> branchMap = BranchMap.map;
//        prop.setProperty("group.id","LC_branch_name_product_payperiod");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                inputTopic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        env.addSource(kafkaConsumer)
//                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(String s) throws Exception {
//                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
//                    }
//                })
//                .filter(x -> x.signdate.length() > 0)
//                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        if(branchMap.keySet().contains(premiumsKafkaEntity04.getBranch_name())){
//                            premiumsKafkaEntity04.setBranch_name(branchMap.get(premiumsKafkaEntity04.getBranch_name()));
//                        }
//                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
//                        return premiumsKafkaEntity04;
//                    }
//                })
//                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple4<String, String,String,String>>() {
//                    @Override
//                    public Tuple4<String, String,String,String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        return Tuple4.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
//                                premiumsKafkaEntity04.getProduct_name(),premiumsKafkaEntity04.getPay_period());
//                    }})
//                .flatMap(new OrganizationLCFlatMap())
//
//                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationProductDetialWithFieldName>() {
//                    @Override
//                    public ApplicationProductDetialWithFieldName map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        String manage_code = manageCom.get(premiumsKafkaEntity04.getBranch_name());
//                        String manage_name = premiumsKafkaEntity04.getBranch_name();
//                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
//                        String product_code = "";
//
//                        if("".equals(premiumsKafkaEntity04.getContplancode()) || premiumsKafkaEntity04.getContplancode() == null){
//                            product_code = premiumsKafkaEntity04.getRiskcode();
//                        }else{
//                            product_code = premiumsKafkaEntity04.getContplancode();
//                        }
//                        String product_name = premiumsKafkaEntity04.getProduct_name();
//                        String pay_period = premiumsKafkaEntity04.getPay_period();
//                        String key_id = day_id +"#" +manage_code +"#" + product_code;
//
//                        ApplicationProductDetialWithFieldName applicationProductDetial = new ApplicationProductDetialWithFieldName();
//                        applicationProductDetial.setManage_name(manage_name);
//                        applicationProductDetial.setManage_code(manage_code);
//                        applicationProductDetial.setDay_id(day_id);
//                        applicationProductDetial.setKey_id(key_id);
//                        applicationProductDetial.setProduct_code(product_code);
//                        applicationProductDetial.setProduct_name(product_name);
//                        applicationProductDetial.setPrem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                        if("0".equals(pay_period)){
//                            applicationProductDetial.setSingle_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setFieldName("key_id,day_id,manage_code,manage_name,product_code,product_name,single_prem_day");
//                        }else if("3".equals(pay_period)){
//                            applicationProductDetial.setThree_year_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setFieldName("key_id,day_id,manage_code,manage_name,product_code,product_name,three_year_prem_day");
//                        }else if("5".equals(pay_period)){
//                            applicationProductDetial.setFive_year_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setFieldName("key_id,day_id,manage_code,manage_name,product_code,product_name,five_year_prem_day");
//                        }else if("10".equals(pay_period)){
//                            applicationProductDetial.setTen_year_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setFieldName("key_id,day_id,manage_code,manage_name,product_code,product_name,ten_year_prem_day");
//                        }else if("20".equals(pay_period)){
//                            applicationProductDetial.setTwenty_year_prem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setFieldName("key_id,day_id,manage_code,manage_name,product_code,product_name,twenty_year_prem_day");
//                        }
//                        return applicationProductDetial;
//                    }
//                })
//                .addSink(new InsertHbaseOnly<>("KLMIDAPPRUN:AppProductDetailLC"));
//    }
//
//
//
//
//
//    public static void LC_branch_name_product_payperiod_num (StreamExecutionEnvironment env,
//                                                         String inputTopic,
//                                                         Properties prop,
//                                                         String outputTopic,
//                                                         String tableName){
//        Map<String, String> manageCom = ManageCom.manageCom;
//        Map<String, String> branchMap = BranchMap.map;
//        prop.setProperty("group.id","LC_branch_name_product_payperiod_num");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                inputTopic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        env.addSource(kafkaConsumer)
//                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(String s) throws Exception {
//                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
//                    }
//                })
//                .filter(x -> x.signdate.length() > 0)
//                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        if(branchMap.keySet().contains(premiumsKafkaEntity04.getBranch_name())){
//                            premiumsKafkaEntity04.setBranch_name(branchMap.get(premiumsKafkaEntity04.getBranch_name()));
//                        }
//                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
//                        return premiumsKafkaEntity04;
//                    }
//                })
//                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple4<String, String,String,String>>() {
//                    @Override
//                    public Tuple4<String, String,String,String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        return Tuple4.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
//                                premiumsKafkaEntity04.getProduct_name(),premiumsKafkaEntity04.getPay_period());
//                    }})
//                .flatMap(new OrganizationLCFlatMapCount())
//
//                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationProductDetialWithColumnName>() {
//                    @Override
//                    public ApplicationProductDetialWithColumnName map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        String manage_code = manageCom.get(premiumsKafkaEntity04.getBranch_name());
//                        String manage_name = premiumsKafkaEntity04.getBranch_name();
//                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
//                        String product_code = "";
//
//                        if("".equals(premiumsKafkaEntity04.getContplancode()) || premiumsKafkaEntity04.getContplancode() == null){
//                            product_code = premiumsKafkaEntity04.getRiskcode();
//                        }else{
//                            product_code = premiumsKafkaEntity04.getContplancode();
//                        }
//                        String product_name = premiumsKafkaEntity04.getProduct_name();
//                        String pay_period = premiumsKafkaEntity04.getPay_period();
//                        String key_id = day_id +"#" +manage_code +"#" + product_code;
//
//                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
//                        applicationProductDetial.setManage_name(manage_name);
//                        applicationProductDetial.setManage_code(manage_code);
//                        applicationProductDetial.setDay_id(day_id);
//                        applicationProductDetial.setKey_id(key_id);
//                        applicationProductDetial.setProduct_code(product_code);
//                        applicationProductDetial.setProduct_name(product_name);
//
//                        if("0".equals(pay_period)){
//                            applicationProductDetial.setNum_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("0");
//                        }else if("3".equals(pay_period)){
//                            applicationProductDetial.setNum_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("3");
//                        }else if("5".equals(pay_period)){
//                            applicationProductDetial.setNum_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("5");
//                        }else if("10".equals(pay_period)){
//                            applicationProductDetial.setNum_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("10");
//                        }else if("20".equals(pay_period)){
//                            applicationProductDetial.setNum_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));
//                            applicationProductDetial.setColumnName("20");
//                        }
//                        return applicationProductDetial;
//                    }
//                })
//                .addSink(new InsertHbaseOnly<>("KLMIDAPPRUN:AppProductDetailLC"));
//    }
//
}
