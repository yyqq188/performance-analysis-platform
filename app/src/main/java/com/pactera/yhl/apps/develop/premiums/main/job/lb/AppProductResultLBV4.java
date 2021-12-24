package com.pactera.yhl.apps.develop.premiums.main.job.lb;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.LbpolKafka06;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductResultWithColumnName;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.ProductLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.ProductLCFlatMapCount;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.ProductLCFlatMapCountContno;
import com.pactera.yhl.apps.develop.premiums.sink.InsertHbaseOnlyV2LB;
import com.pactera.yhl.constract.BranchMap;
import com.pactera.yhl.constract.ManageCom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class AppProductResultLBV4 {
    public static void LB_branch_name_product (StreamExecutionEnvironment env,
                                               String inputTopic,
                                               Properties prop,
                                               String outputTopic,
                                               String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LB_branch_name_product");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka06>() {
                    @Override
                    public LbpolKafka06 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka06.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<LbpolKafka06, LbpolKafka06>() {
                    @Override
                    public LbpolKafka06 map(LbpolKafka06 lbpolKafka06) throws Exception {
                        if(branchMap.keySet().contains(lbpolKafka06.getBranch_name())){
                            lbpolKafka06.setBranch_name(branchMap.get(lbpolKafka06.getBranch_name()));
                        }

                        String day_id = "";
                        if(lbpolKafka06.getModifydate().equals("") ||
                                lbpolKafka06.getModifydate().length() == 0 ||
                                !Objects.isNull(lbpolKafka06.getModifydate())){
                            day_id = lbpolKafka06.getModifydate().split("\\s+")[0];
                        }
                        if(day_id.equals("") || day_id.length()==0){
                            day_id = lbpolKafka06.getSigndate().split("\\s+")[0];
                        }
                        lbpolKafka06.setSigndate(day_id);

                        return lbpolKafka06;
                    }
                })
                .keyBy(new KeySelector<LbpolKafka06, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(LbpolKafka06 lbpolKafka06) throws Exception {
                        return Tuple2.of(lbpolKafka06.getSigndate(), lbpolKafka06.getBranch_name());
                    }})
                .flatMap(new ProductLCFlatMap())

                .map(new MapFunction<LbpolKafka06, ApplicationProductResultWithColumnName>() {
                    @Override
                    public ApplicationProductResultWithColumnName map(LbpolKafka06 lbpolKafka06) throws Exception {
                        String manage_code = manageCom.get(lbpolKafka06.getBranch_name());
                        String manage_name = lbpolKafka06.getBranch_name();
                        String day_id = lbpolKafka06.getSigndate();
                        String product_code = "";
                        if("".equals(lbpolKafka06.getContplancode()) || lbpolKafka06.getContplancode() == null){
                            product_code = lbpolKafka06.getRiskcode();
                        }else{
                            product_code = lbpolKafka06.getContplancode();
                        }
                        String product_name = lbpolKafka06.getProduct_name();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductResultWithColumnName applicationProductResult = new ApplicationProductResultWithColumnName();
                        applicationProductResult.setManage_name(manage_name);
                        applicationProductResult.setManage_code(manage_code);
                        applicationProductResult.setDay_id(day_id);
                        applicationProductResult.setKey_id(key_id);
                        applicationProductResult.setProduct_code(product_code);
                        applicationProductResult.setProduct_name(product_name);
                        applicationProductResult.setPrem_day(Double.valueOf(lbpolKafka06.getPrem()));

                        applicationProductResult.setColumnName("prem_day;prem_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        return applicationProductResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2LB<>("KLMIDAPPRUN:AppProductResultLC","APPLICATION_PRODUCT_RESULT_RT"));
    }

    public static void LB_branch_name_num_product(StreamExecutionEnvironment env,
                                                  String inputTopic,
                                                  Properties prop,
                                                  String outputTopic,
                                                  String tableName){

        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LB_branch_name");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka06>() {
                    @Override
                    public LbpolKafka06 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka06.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<LbpolKafka06, LbpolKafka06>() {
                    @Override
                    public LbpolKafka06 map(LbpolKafka06 lbpolKafka06) throws Exception {
                        if(branchMap.keySet().contains(lbpolKafka06.getBranch_name())){
                            lbpolKafka06.setBranch_name(branchMap.get(lbpolKafka06.getBranch_name()));
                        }

                        String day_id = "";
                        if(lbpolKafka06.getModifydate().equals("") ||
                                lbpolKafka06.getModifydate().length() == 0 ||
                                !Objects.isNull(lbpolKafka06.getModifydate())){
                            day_id = lbpolKafka06.getModifydate().split("\\s+")[0];
                        }
                        if(day_id.equals("") || day_id.length()==0){
                            day_id = lbpolKafka06.getSigndate().split("\\s+")[0];
                        }
                        lbpolKafka06.setSigndate(day_id);


                        return lbpolKafka06;
                    }
                })
                .keyBy(new KeySelector<LbpolKafka06, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(LbpolKafka06 lbpolKafka06) throws Exception {
                        return Tuple2.of(lbpolKafka06.getSigndate(),
                                lbpolKafka06.getBranch_name());
                    }})
                .flatMap(new ProductLCFlatMapCountContno())

                .map(new MapFunction<LbpolKafka06, ApplicationProductResultWithColumnName>() {
                    @Override
                    public ApplicationProductResultWithColumnName map(LbpolKafka06 lbpolKafka06) throws Exception {
                        String manage_code = manageCom.get(lbpolKafka06.getBranch_name());
                        String manage_name = lbpolKafka06.getBranch_name();
                        String day_id = lbpolKafka06.getSigndate();
                        String product_code = "";
                        if("".equals(lbpolKafka06.getContplancode()) || lbpolKafka06.getContplancode() == null){
                            product_code = lbpolKafka06.getRiskcode();
                        }else{
                            product_code = lbpolKafka06.getContplancode();
                        }
                        String product_name = lbpolKafka06.getProduct_name();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductResultWithColumnName applicationProductResult = new ApplicationProductResultWithColumnName();
                        applicationProductResult.setManage_name(manage_name);
                        applicationProductResult.setManage_code(manage_code);
                        applicationProductResult.setDay_id(day_id);
                        applicationProductResult.setKey_id(key_id);
                        applicationProductResult.setProduct_code(product_code);
                        applicationProductResult.setProduct_name(product_name);
                        applicationProductResult.setNum_day(Double.valueOf(lbpolKafka06.getContno()));

                        applicationProductResult.setColumnName("num_day;num_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        return applicationProductResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2LB<>("KLMIDAPPRUN:AppProductResultLC","APPLICATION_PRODUCT_RESULT_RT"));

    }
    public static void LB_all_num_product(StreamExecutionEnvironment env,
                                                  String inputTopic,
                                                  Properties prop,
                                                  String outputTopic,
                                                  String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LB_all_num_product");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka06>() {
                    @Override
                    public LbpolKafka06 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka06.class);
                    }
                })
                .filter(x -> x.signdate.length() > 0)
                .map(new MapFunction<LbpolKafka06, LbpolKafka06>() {
                    @Override
                    public LbpolKafka06 map(LbpolKafka06 lbpolKafka06) throws Exception {
                        if(branchMap.keySet().contains(lbpolKafka06.getBranch_name())){
                            lbpolKafka06.setBranch_name(branchMap.get(lbpolKafka06.getBranch_name()));
                        }

                        String day_id = "";
                        if(lbpolKafka06.getModifydate().equals("") ||
                                lbpolKafka06.getModifydate().length() == 0 ||
                                !Objects.isNull(lbpolKafka06.getModifydate())){
                            day_id = lbpolKafka06.getModifydate().split("\\s+")[0];
                        }
                        if(day_id.equals("") || day_id.length()==0){
                            day_id = lbpolKafka06.getSigndate().split("\\s+")[0];
                        }
                        lbpolKafka06.setSigndate(day_id);


                        return lbpolKafka06;
                    }
                })
                .keyBy(new KeySelector<LbpolKafka06, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(LbpolKafka06 lbpolKafka06) throws Exception {
                        return Tuple2.of(lbpolKafka06.getSigndate(),
                                "总公司");
                    }})
                .flatMap(new ProductLCFlatMapCountContno())

                .map(new MapFunction<LbpolKafka06, ApplicationProductResultWithColumnName>() {
                    @Override
                    public ApplicationProductResultWithColumnName map(LbpolKafka06 lbpolKafka06) throws Exception {
                        String manage_code = "86";
                        String manage_name = "总公司";
                        String day_id = lbpolKafka06.getSigndate();
                        String product_code = "";
                        if("".equals(lbpolKafka06.getContplancode()) || lbpolKafka06.getContplancode() == null){
                            product_code = lbpolKafka06.getRiskcode();
                        }else{
                            product_code = lbpolKafka06.getContplancode();
                        }
                        String product_name = lbpolKafka06.getProduct_name();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductResultWithColumnName applicationProductResult = new ApplicationProductResultWithColumnName();
                        applicationProductResult.setManage_name(manage_name);
                        applicationProductResult.setManage_code(manage_code);
                        applicationProductResult.setDay_id(day_id);
                        applicationProductResult.setKey_id(key_id);
                        applicationProductResult.setProduct_code(product_code);
                        applicationProductResult.setProduct_name(product_name);
                        applicationProductResult.setNum_day(Double.valueOf(lbpolKafka06.getContno()));

                        applicationProductResult.setColumnName("num_day;num_day;key_id,day_id,manage_code,manage_name,product_code,product_name");
                        return applicationProductResult;
                    }
                })
                .addSink(new InsertHbaseOnlyV2LB<>("KLMIDAPPRUN:AppProductResultLC","APPLICATION_PRODUCT_RESULT_RT"));

    }
}
