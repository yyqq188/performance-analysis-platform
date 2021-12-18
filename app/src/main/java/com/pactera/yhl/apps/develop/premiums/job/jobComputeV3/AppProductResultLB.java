package com.pactera.yhl.apps.develop.premiums.job.jobComputeV3;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.LbpolKafka05;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductResult;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductResultWithFieldColumnName;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.ProductLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.ProductLCFlatMapCount;
import com.pactera.yhl.apps.develop.premiums.sink.InsertClickhousePassHbase;
import com.pactera.yhl.apps.develop.premiums.sink.PremiumsClickhouseSink;
import com.pactera.yhl.constract.BranchMap;
import com.pactera.yhl.constract.ManageCom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

public class AppProductResultLB {
//
//    //todo 浙江分公司 产品
//    public static void LB_branch_name_product (StreamExecutionEnvironment env,
//                                       String inputTopic,
//                                       Properties prop,
//                                       String outputTopic,
//                                       String tableName){
//        Map<String, String> manageCom = ManageCom.manageCom;
//        Map<String, String> branchMap = BranchMap.map;
//        prop.setProperty("group.id","LB_branch_name");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                inputTopic,
//                new SimpleStringSchema(),
//                prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        env.addSource(kafkaConsumer)
//                .map(new MapFunction<String, LbpolKafka05>() {
//                    @Override
//                    public LbpolKafka05 map(String s) throws Exception {
//                        return JSON.parseObject(s,LbpolKafka05.class);
//                    }
//                })
//                .filter(x -> x.signdate.length() > 0)
//                .map(new MapFunction<LbpolKafka05, LbpolKafka05>() {
//                    @Override
//                    public LbpolKafka05 map(LbpolKafka05 lbpolKafka05) throws Exception {
//                        if(branchMap.keySet().contains(lbpolKafka05.getBranch_name())){
//                            lbpolKafka05.setBranch_name(branchMap.get(lbpolKafka05.getBranch_name()));
//                        }
//                        return lbpolKafka05;
//                    }
//                })
//                .keyBy(new KeySelector<LbpolKafka05, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(LbpolKafka05 lbpolKafka05) throws Exception {
//                        return Tuple2.of(lbpolKafka05.getSigndate(), lbpolKafka05.getBranch_name());
//                    }})
//                .flatMap(new ProductLCFlatMap())
//
//                .map(new MapFunction<LbpolKafka05, ApplicationProductResultWithFieldColumnName>() {
//                    @Override
//                    public ApplicationProductResultWithFieldColumnName map(LbpolKafka05 lbpolKafka05) throws Exception {
//                        String manage_code = manageCom.get(lbpolKafka05.getBranch_name());
//                        String manage_name = lbpolKafka05.getBranch_name();
//                        String day_id = lbpolKafka05.getSigndate().split("\\s+")[0];
//                        String product_code = "";
//                        if("".equals(lbpolKafka05.getContplancode()) || lbpolKafka05.getContplancode() == null){
//                            product_code = lbpolKafka05.getRiskcode();
//                        }else{
//                            product_code = lbpolKafka05.getContplancode();
//                        }
//                        String product_name = lbpolKafka05.getProduct_name();
//                        String key_id = day_id +"#" +manage_code +"#" + product_code;
//
//                        ApplicationProductResultWithFieldColumnName applicationProductResult = new ApplicationProductResultWithFieldColumnName();
//                        applicationProductResult.setManage_name(manage_name);
//                        applicationProductResult.setManage_code(manage_code);
//                        applicationProductResult.setDay_id(day_id);
//                        applicationProductResult.setKey_id(key_id);
//                        applicationProductResult.setProduct_code(product_code);
//                        applicationProductResult.setProduct_name(product_name);
//                        applicationProductResult.setPrem_day(Double.valueOf(lbpolKafka05.getPrem()));
//
//                        applicationProductResult.setFieldName("key_id,day_id,manage_code,manage_name,product_code,product_name,prem_day");
//                        applicationProductResult.setColumnName("new_prem");
//                        applicationProductResult.setValueField("prem_day,prem_day,prem_day");
//                        return applicationProductResult;
//                    }
//                })
//                .addSink(new InsertClickhousePassHbase<>("APPLICATION_PRODUCT_RESULT_RT",
//                        "KLMIDAPPRUN:AppProductResultLC"));
//    }
//
//
//    public static void LB_branch_name_num_product(StreamExecutionEnvironment env,
//                                       String inputTopic,
//                                       Properties prop,
//                                       String outputTopic,
//                                       String tableName){
//
//        Map<String, String> manageCom = ManageCom.manageCom;
//        Map<String, String> branchMap = BranchMap.map;
//        prop.setProperty("group.id","LB_branch_name");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                inputTopic,
//                new SimpleStringSchema(),
//                prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        env.addSource(kafkaConsumer)
//                .map(new MapFunction<String, LbpolKafka05>() {
//                    @Override
//                    public LbpolKafka05 map(String s) throws Exception {
//                        return JSON.parseObject(s,LbpolKafka05.class);
//                    }
//                })
//                .filter(x -> x.signdate.length() > 0)
//                .map(new MapFunction<LbpolKafka05, LbpolKafka05>() {
//                    @Override
//                    public LbpolKafka05 map(LbpolKafka05 lbpolKafka05) throws Exception {
//                        if(branchMap.keySet().contains(lbpolKafka05.getBranch_name())){
//                            lbpolKafka05.setBranch_name(branchMap.get(lbpolKafka05.getBranch_name()));
//                        }
//                        return lbpolKafka05;
//                    }
//                })
//                .keyBy(new KeySelector<LbpolKafka05, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(LbpolKafka05 lbpolKafka05) throws Exception {
//                        return Tuple2.of(lbpolKafka05.getSigndate(), lbpolKafka05.getBranch_name());
//                    }})
//                .flatMap(new ProductLCFlatMapCount())
//
//                .map(new MapFunction<LbpolKafka05, ApplicationProductResultWithFieldColumnName>() {
//                    @Override
//                    public ApplicationProductResultWithFieldColumnName map(LbpolKafka05 lbpolKafka05) throws Exception {
//                        String manage_code = manageCom.get(lbpolKafka05.getBranch_name());
//                        String manage_name = lbpolKafka05.getBranch_name();
//                        String day_id = lbpolKafka05.getSigndate().split("\\s+")[0];
//                        String product_code = "";
//                        if("".equals(lbpolKafka05.getContplancode()) || lbpolKafka05.getContplancode() == null){
//                            product_code = lbpolKafka05.getRiskcode();
//                        }else{
//                            product_code = lbpolKafka05.getContplancode();
//                        }
//                        String product_name = lbpolKafka05.getProduct_name();
//                        String key_id = day_id +"#" +manage_code +"#" + product_code;
//
//                        ApplicationProductResultWithFieldColumnName applicationProductResult = new ApplicationProductResultWithFieldColumnName();
//                        applicationProductResult.setManage_name(manage_name);
//                        applicationProductResult.setManage_code(manage_code);
//                        applicationProductResult.setDay_id(day_id);
//                        applicationProductResult.setKey_id(key_id);
//                        applicationProductResult.setProduct_code(product_code);
//                        applicationProductResult.setProduct_name(product_name);
//                        applicationProductResult.setNum_day(Double.valueOf(lbpolKafka05.getPrem()));
//
//                        applicationProductResult.setFieldName("key_id,day_id,manage_code,manage_name,product_code,product_name,num_day");
//                        applicationProductResult.setColumnName("new_count");
//                        applicationProductResult.setValueField("num_day,num_day,num_day");
//                        return applicationProductResult;
//                    }
//                })
//                .addSink(new InsertClickhousePassHbase<>("APPLICATION_PRODUCT_RESULT_RT",
//                        "KLMIDAPPRUN:AppProductResultLC"));
//
//    }
//
}
