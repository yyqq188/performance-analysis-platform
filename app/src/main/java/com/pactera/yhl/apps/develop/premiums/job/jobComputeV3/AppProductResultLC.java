package com.pactera.yhl.apps.develop.premiums.job.jobComputeV3;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResult;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductResult;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.sink.InsertKafkaOnly;
import com.pactera.yhl.apps.develop.premiums.sink.PremiumsClickhouseSink;
import com.pactera.yhl.constract.ManageCom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

public class AppProductResultLC {

    //todo 浙江分公司 产品
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

                .map(new MapFunction<PremiumsKafkaEntity04, ApplicationProductResult>() {
                    @Override
                    public ApplicationProductResult map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        String manage_code = manageCom.get(premiumsKafkaEntity04.getBranch_name());
                        String manage_name = premiumsKafkaEntity04.getBranch_name();
                        String day_id = premiumsKafkaEntity04.getSigndate().split("\\s+")[0];
                        String product_code = "";

                        if("".equals(premiumsKafkaEntity04.getContplancode()) || premiumsKafkaEntity04.getContplancode() == null){
                            product_code = premiumsKafkaEntity04.getRiskcode();
                        }else{
                            product_code = premiumsKafkaEntity04.getContplancode();
                        }
                        String product_name = premiumsKafkaEntity04.getProduct_name();
                        String key_id = day_id +"#" +manage_code +"#" + product_code;

                        ApplicationProductResult applicationProductResult = new ApplicationProductResult();
                        applicationProductResult.setManage_name(manage_name);
                        applicationProductResult.setManage_code(manage_code);
                        applicationProductResult.setDay_id(day_id);
                        applicationProductResult.setKey_id(key_id);
                        applicationProductResult.setProduct_code(product_code);
                        applicationProductResult.setProduct_name(product_name);
                        applicationProductResult.setPrem_day(Double.valueOf(premiumsKafkaEntity04.getPrem()));

                        return applicationProductResult;
                    }
                })

//                .addSink(new InsertKafkaOnly<>(outputTopic));
                .addSink(new PremiumsClickhouseSink(tableName));
    }

}
