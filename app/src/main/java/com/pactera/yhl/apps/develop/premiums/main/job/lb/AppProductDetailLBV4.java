package com.pactera.yhl.apps.develop.premiums.main.job.lb;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.LbpolKafka05;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductDetialWithColumnName;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductDetialWithFieldColumnName;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.ProductLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.ProductLCFlatMapCount;
import com.pactera.yhl.apps.develop.premiums.sink.InsertClickhousePassHbase;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnlyV2;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnlyV2LB;
import com.pactera.yhl.constract.BranchMap;
import com.pactera.yhl.constract.ManageCom;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

public class AppProductDetailLBV4 {

    public static void LB_branch_name_product_payperiod_num (StreamExecutionEnvironment env,
                                                             String inputTopic,
                                                             Properties prop,
                                                             String outputTopic,
                                                             String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LB_branch_name_product_payperiod_num");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka05>() {
                    @Override
                    public LbpolKafka05 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .filter(new FilterFunction<LbpolKafka05>() {
                    @Override
                    public boolean filter(LbpolKafka05 lbpolKafka05) throws Exception {
                        if(lbpolKafka05.getPeriod_type() == null || lbpolKafka05.getPeriod_type().length() == 0){
                            return false;
                        }else{
                            return true;
                        }
                    }
                })
                .map(new MapFunction<LbpolKafka05, LbpolKafka05>() {
                    @Override
                    public LbpolKafka05 map(LbpolKafka05 lbpolKafka05) throws Exception {
                        if(branchMap.keySet().contains(lbpolKafka05.getBranch_name())){
                            lbpolKafka05.setBranch_name(branchMap.get(lbpolKafka05.getBranch_name()));
                        }
                        lbpolKafka05.setBranch_id("86" + lbpolKafka05.getBranch_id().substring(0,2));
                        return lbpolKafka05;
                    }
                })
                .keyBy(new KeySelector<LbpolKafka05, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> getKey(LbpolKafka05 lbpolKafka05) throws Exception {
                        return Tuple4.of(lbpolKafka05.getSigndate(), lbpolKafka05.getBranch_name(),
                                lbpolKafka05.getProduct_name(),lbpolKafka05.getPay_period());
                    }})
                .flatMap(new ProductLCFlatMapCount())

                .map(new MapFunction<LbpolKafka05, ApplicationProductDetialWithColumnName>() {
                    @Override
                    public ApplicationProductDetialWithColumnName map(LbpolKafka05 lbpolKafka05) throws Exception {
                        String manage_code = manageCom.get(lbpolKafka05.getBranch_name());
                        String manage_name = lbpolKafka05.getBranch_name();
                        String day_id = lbpolKafka05.getSigndate().split("\\s+")[0];
                        String product_code = "";

                        if("".equals(lbpolKafka05.getContplancode()) || lbpolKafka05.getContplancode() == null){
                            product_code = lbpolKafka05.getRiskcode();
                        }else{
                            product_code = lbpolKafka05.getContplancode();
                        }
                        String product_name = lbpolKafka05.getProduct_name();
                        String pay_period = lbpolKafka05.getPay_period();

                        String key_id = day_id +"#" +manage_code +"#" + product_code;
                        System.out.println("keyid== " + key_id);
                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
                        applicationProductDetial.setManage_name(manage_name);
                        applicationProductDetial.setManage_code(manage_code);
                        applicationProductDetial.setDay_id(day_id);
                        applicationProductDetial.setKey_id(key_id);
                        applicationProductDetial.setProduct_code(product_code);
                        applicationProductDetial.setProduct_name(product_name);
                        applicationProductDetial.setNum_day(Double.valueOf(lbpolKafka05.getPrem()));
                        applicationProductDetial.setNum_day(Double.valueOf(lbpolKafka05.getPrem()));
                        applicationProductDetial.setColumnName("num_day;num_day;key_id,day_id,manage_code,manage_name,product_code,product_name");

                        return applicationProductDetial;

                    }
                })
                .addSink(new InsertHbaseOnlyV2LB<>("KLMIDAPPRUN:AppProductDetailLC","APPLICATION_PRODUCT_DETIAL_RT"));

    }

    public static void LB_branch_name_product_payperiod (StreamExecutionEnvironment env,
                                                         String inputTopic,
                                                         Properties prop,
                                                         String outputTopic,
                                                         String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LB_branch_name_product_payperiod");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka05>() {
                    @Override
                    public LbpolKafka05 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka05.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<LbpolKafka05, LbpolKafka05>() {
                    @Override
                    public LbpolKafka05 map(LbpolKafka05 lbpolKafka05) throws Exception {
                        if(branchMap.keySet().contains(lbpolKafka05.getBranch_name())){
                            lbpolKafka05.setBranch_name(branchMap.get(lbpolKafka05.getBranch_name()));
                        }
                        lbpolKafka05.setBranch_id("86" + lbpolKafka05.getBranch_id().substring(0,2));
                        return lbpolKafka05;
                    }
                })
                .keyBy(new KeySelector<LbpolKafka05, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> getKey(LbpolKafka05 lbpolKafka05) throws Exception {
                        return Tuple4.of(lbpolKafka05.getSigndate(), lbpolKafka05.getBranch_name(),
                                lbpolKafka05.getProduct_name(),lbpolKafka05.getPay_period());
                    }})
                .flatMap(new ProductLCFlatMap())
                .filter(new FilterFunction<LbpolKafka05>() {
                    @Override
                    public boolean filter(LbpolKafka05 lbpolKafka05) throws Exception {
                        if(lbpolKafka05.getPeriod_type() == null || lbpolKafka05.getPeriod_type().length() == 0){
                            return false;
                        }else{
                            return true;
                        }
                    }
                })
                .map(new MapFunction<LbpolKafka05, ApplicationProductDetialWithColumnName>() {
                    @Override
                    public ApplicationProductDetialWithColumnName map(LbpolKafka05 lbpolKafka05) throws Exception {
                        System.out.println("lbpolKafka05.getBranch_name() " + lbpolKafka05.getBranch_name());
                        String manage_code = manageCom.get(lbpolKafka05.getBranch_name());
                        String manage_name = lbpolKafka05.getBranch_name();
                        String day_id = lbpolKafka05.getSigndate().split("\\s+")[0];
                        String product_code = "";

                        if("".equals(lbpolKafka05.getContplancode()) || lbpolKafka05.getContplancode() == null){
                            product_code = lbpolKafka05.getRiskcode();
                        }else{
                            product_code = lbpolKafka05.getContplancode();
                        }
                        String product_name = lbpolKafka05.getProduct_name();
                        String pay_period = lbpolKafka05.getPay_period();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductDetialWithColumnName applicationProductDetial = new ApplicationProductDetialWithColumnName();
                        applicationProductDetial.setManage_name(manage_name);
                        applicationProductDetial.setManage_code(manage_code);
                        applicationProductDetial.setDay_id(day_id);
                        applicationProductDetial.setKey_id(key_id);
                        applicationProductDetial.setProduct_code(product_code);
                        applicationProductDetial.setProduct_name(product_name);
                        applicationProductDetial.setPrem_day(Double.valueOf(lbpolKafka05.getPrem()));
                        if("0".equals(pay_period)){
                            applicationProductDetial.setSingle_prem_day(Double.valueOf(lbpolKafka05.getPrem()));
                            applicationProductDetial.setColumnName("single_prem_day;single_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("3".equals(pay_period)){
                            applicationProductDetial.setThree_year_prem_day(Double.valueOf(lbpolKafka05.getPrem()));
                            applicationProductDetial.setColumnName("three_year_prem_day;three_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("5".equals(pay_period)){
                            applicationProductDetial.setFive_year_prem_day(Double.valueOf(lbpolKafka05.getPrem()));
                            applicationProductDetial.setColumnName("five_year_prem_day;five_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("10".equals(pay_period)){
                            applicationProductDetial.setTen_year_prem_day(Double.valueOf(lbpolKafka05.getPrem()));
                            applicationProductDetial.setColumnName("ten_year_prem_day;ten_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }else if("20".equals(pay_period)){
                            applicationProductDetial.setTwenty_year_prem_day(Double.valueOf(lbpolKafka05.getPrem()));
                            applicationProductDetial.setColumnName("twenty_year_prem_day;twenty_year_prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        }
                        return applicationProductDetial;
                    }
                })
                .addSink(new InsertHbaseOnlyV2LB<>("KLMIDAPPRUN:AppProductDetailLC","APPLICATION_PRODUCT_DETIAL_RT"));
    }
}
