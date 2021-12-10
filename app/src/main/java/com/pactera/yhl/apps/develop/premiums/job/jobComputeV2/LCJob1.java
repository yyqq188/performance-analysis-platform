package com.pactera.yhl.apps.develop.premiums.job.jobComputeV2;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap.OrganizationLCFlatMap;
import com.pactera.yhl.apps.develop.premiums.sink.InsertKafkaOnly;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Properties;

public class LCJob1 {
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    //todo  浙江分公司
    public static void LC_branch_name (StreamExecutionEnvironment env,
                            String inputTopic,
                            Properties prop,String outputTopic){
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
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name());
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity04, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),premiumsKafkaEntity04.getPrem());
                    }
                })
                .addSink(new InsertKafkaOnly<>(outputTopic));
    }


//    public static void LC_branch_name (StreamExecutionEnvironment env,
//                                       String inputTopic,
//                                       Properties prop,String outputTopic){
//        prop.setProperty("group.id","LC_branch_name");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                inputTopic,
//                new SimpleStringSchema(),
//                prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        env.addSource(kafkaConsumer)
//                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 map(String s) throws Exception {
//                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
//                    }
//                })
//                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        return Tuple2.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name());
//                    }})
//                .reduce(new ReduceFunction<PremiumsKafkaEntity04>() {
//                    @Override
//                    public PremiumsKafkaEntity04 reduce(PremiumsKafkaEntity04 v1, PremiumsKafkaEntity04 v2) throws Exception {
//                        PremiumsKafkaEntity04 newLc = new PremiumsKafkaEntity04();
//                        BeanCopier beanCopier = BeanCopier.create(v1.getClass(), newLc.getClass(), false);
//                        beanCopier.copy(v1,newLc,null);
//                        Double pre = Double.valueOf(v1.getPrem());
//                        Double suf = Double.valueOf(v2.getPrem());
//                        newLc.setPrem(String.valueOf(pre + suf));
//                        return newLc;
//                    }
//                })
//                .map(new MapFunction<PremiumsKafkaEntity04, Tuple3<String, String,String>>() {
//                    @Override
//                    public Tuple3<String, String,String> map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
//                        return Tuple3.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),premiumsKafkaEntity04.getPrem());
//                    }
//                })
//                .addSink(new InsertKafkaOnly<>(outputTopic));
//    }

    //todo 宁波
    public static void LC_workarea (StreamExecutionEnvironment env,
                                       String inputTopic,
                                       Properties prop,String outputTopic){
        prop.setProperty("group.id","LC_workarea");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple2.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getWorkarea());
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity04, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getWorkarea(),premiumsKafkaEntity04.getPrem());
                    }
                })
                .addSink(new InsertKafkaOnly<>(outputTopic));
    }
    //todo 浙江分公司 产品
    public static void LC_branch_name_product (StreamExecutionEnvironment env,
                                       String inputTopic,
                                       Properties prop,String outputTopic){
        prop.setProperty("group.id","LC_branch_name_product");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
                                premiumsKafkaEntity04.getProduct_name());
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity04, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple4.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),premiumsKafkaEntity04.getProduct_name(),premiumsKafkaEntity04.getPrem());
                    }
                })
                .addSink(new InsertKafkaOnly<>(outputTopic));
    }
    //todo 浙江分公司 期趸交
    public static void LC_branch_name_periodtype (StreamExecutionEnvironment env,
                                               String inputTopic,
                                               Properties prop,String outputTopic){
        prop.setProperty("group.id","LC_branch_name_payperiod");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
                                premiumsKafkaEntity04.getPeriod_type());
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity04, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple4.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
                                premiumsKafkaEntity04.getPeriod_type(),
                                premiumsKafkaEntity04.getPrem());
                    }
                })
                .addSink(new InsertKafkaOnly<>(outputTopic));
    }
    //todo 宁波 期趸交
    public static void LC_workarea_periodtype (StreamExecutionEnvironment env,
                                                         String inputTopic,
                                                         Properties prop,String outputTopic){
        prop.setProperty("group.id","LC_workarea_payperiod");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple3.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getWorkarea(),
                                premiumsKafkaEntity04.getPeriod_type());
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity04, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple4.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getWorkarea(),
                                premiumsKafkaEntity04.getPeriod_type(),
                                premiumsKafkaEntity04.getPrem());
                    }
                })
                .addSink(new InsertKafkaOnly<>(outputTopic));
    }

    //todo 浙江 产品 年期
    public static void LC_branch_name_product_payperiod (StreamExecutionEnvironment env,
                                               String inputTopic,
                                               Properties prop,String outputTopic){
        prop.setProperty("group.id","LC_branch_name_product_payperiod");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity04.class);
                    }
                })
                .map(new MapFunction<PremiumsKafkaEntity04, PremiumsKafkaEntity04>() {
                    @Override
                    public PremiumsKafkaEntity04 map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        if("宁波支公司".equals(premiumsKafkaEntity04.getBranch_name())){
                            premiumsKafkaEntity04.setBranch_name("浙江分公司");
                        }
                        premiumsKafkaEntity04.setBranch_id("86" + premiumsKafkaEntity04.getBranch_id().substring(0,2));
                        return premiumsKafkaEntity04;
                    }
                })
                .keyBy(new KeySelector<PremiumsKafkaEntity04, Tuple4<String, String,String,String>>() {
                    @Override
                    public Tuple4<String, String,String,String> getKey(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple4.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
                                premiumsKafkaEntity04.getProduct_name(),premiumsKafkaEntity04.getPay_period());
                    }})
                .flatMap(new OrganizationLCFlatMap())
                .map(new MapFunction<PremiumsKafkaEntity04, Tuple5<String, String,String,String,String>>() {
                    @Override
                    public Tuple5<String, String,String,String,String> map(PremiumsKafkaEntity04 premiumsKafkaEntity04) throws Exception {
                        return Tuple5.of(premiumsKafkaEntity04.getSigndate(), premiumsKafkaEntity04.getBranch_name(),
                                premiumsKafkaEntity04.getProduct_name(), premiumsKafkaEntity04.getPay_period(),
                                premiumsKafkaEntity04.getPrem());
                    }
                })
                .addSink(new InsertKafkaOnly<>(outputTopic));
    }


}
