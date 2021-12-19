package com.pactera.yhl.apps.develop.premiums.main.job.lb;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.LbpolKafka06;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResultWithColumnName;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResultWithFieldColumnName;
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class AppGeneralResultLBV4 {
    public static void LB_branch_name (StreamExecutionEnvironment env,
                                       String inputTopic,
                                       Properties prop,
                                       String outputTopic,
                                       String CKTableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LB_branch_name");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        SingleOutputStreamOperator<ApplicationGeneralResultWithColumnName> source = env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka06>() {
                    @Override
                    public LbpolKafka06 map(String s) throws Exception {
                        return JSON.parseObject(s, LbpolKafka06.class);
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
                    }
                })
                .flatMap(new ProductLCFlatMap())
                .map(new MapFunction<LbpolKafka06, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(LbpolKafka06 lbpolKafka06) throws Exception {
                        String manage_code = manageCom.get(lbpolKafka06.getBranch_name());
                        String manage_name = lbpolKafka06.getBranch_name();

//                        String day_id = "";
//                        if(lbpolKafka06.getModifydate().equals("") ||
//                                lbpolKafka06.getModifydate().length() == 0 ||
//                        !Objects.isNull(lbpolKafka06.getModifydate())){
//                            day_id = lbpolKafka06.getModifydate().split("\\s+")[0];
//                        }
//                        if(day_id.equals("") || day_id.length()==0){
//                            day_id = lbpolKafka06.getSigndate().split("\\s+")[0];
//                        }
                        String day_id = lbpolKafka06.getSigndate();


                        String key_id = day_id + "#" + manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setHesi_prem_day(Double.valueOf(lbpolKafka06.getPrem()));
                        applicationGeneralResult.setPrem_new_day(Double.valueOf(lbpolKafka06.getPrem()));
                        applicationGeneralResult.setApprove_prem_day(Double.valueOf(lbpolKafka06.getPrem()));
                        applicationGeneralResult.setColumnName("prem_new_day,approve_prem_day;hesi_prem_day;key_id,day_id,manage_code,manage_name");

                        //正对decimal类型得处理，不能是null需要专门给0.0
                        return applicationGeneralResult;
                    }
                });
//                source.addSink(new PremiumsClickhouseSink(CKTableName));
        source.addSink(new InsertHbaseOnlyV2LB<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }


    public static void LB_branch_name_periodtype (StreamExecutionEnvironment env,
                                                  String inputTopic,
                                                  Properties prop,
                                                  String outputTopic,
                                                  String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LB_branch_name_periodtype");
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
                .keyBy(new KeySelector<LbpolKafka06, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(LbpolKafka06 lbpolKafka06) throws Exception {
                        return Tuple3.of(lbpolKafka06.getSigndate(), lbpolKafka06.getBranch_name(),
                                lbpolKafka06.getProduct_payintv());
                    }})
                .flatMap(new ProductLCFlatMap())
                .filter(new FilterFunction<LbpolKafka06>() {
                    @Override
                    public boolean filter(LbpolKafka06 lbpolKafka06) throws Exception {
                        String product_payintv = lbpolKafka06.getProduct_payintv();
                        if(product_payintv == null || product_payintv.length() == 0 ){
                            return false;
                        }else{
                            return true;
                        }
                    }
                })
                .map(new MapFunction<LbpolKafka06, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(LbpolKafka06 lbpolKafka06) throws Exception {
                        String manage_code = manageCom.get(lbpolKafka06.getBranch_name());
                        String manage_name = lbpolKafka06.getBranch_name();
                        String period_type = lbpolKafka06.getProduct_payintv();

                        String day_id = lbpolKafka06.getSigndate();
                        String key_id = day_id +"#" +manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        if("期缴".equals(period_type.trim())){
                            applicationGeneralResult.setRegular_prem_day(Double.valueOf(lbpolKafka06.getPrem()));
                            applicationGeneralResult.setColumnName("regular_prem_day;regular_prem_day;key_id,day_id,manage_code,manage_name");
                        }else if("趸缴".equals(period_type.trim())){
                            applicationGeneralResult.setSingle_prem_day(Double.valueOf(lbpolKafka06.getPrem()));
                            applicationGeneralResult.setColumnName("single_prem_day;single_prem_day;key_id,day_id,manage_code,manage_name");
                        }
                        return applicationGeneralResult;
                    }
                })
//                .addSink(new InsertHbaseOnly<>("KLMIDAPPRUN:AppGeneralResultLC"));

                .addSink(new InsertHbaseOnlyV2LB<>("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));
    }

    public static void LB_branch_name_num (StreamExecutionEnvironment env,
                                           String inputTopic,
                                           Properties prop,
                                           String outputTopic,
                                           String tableName){
        Map<String, String> manageCom = ManageCom.manageCom;
        Map<String, String> branchMap = BranchMap.map;
        prop.setProperty("group.id","LB_branch_name_num");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        SingleOutputStreamOperator<ApplicationGeneralResultWithColumnName> source = env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka06>() {
                    @Override
                    public LbpolKafka06 map(String s) throws Exception {
                        return JSON.parseObject(s, LbpolKafka06.class);
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
                    }
                })
                .flatMap(new ProductLCFlatMapCount())
                .map(new MapFunction<LbpolKafka06, ApplicationGeneralResultWithColumnName>() {
                    @Override
                    public ApplicationGeneralResultWithColumnName map(LbpolKafka06 lbpolKafka06) throws Exception {
                        String manage_code = manageCom.get(lbpolKafka06.getBranch_name());
                        String manage_name = lbpolKafka06.getBranch_name();

                        String day_id = lbpolKafka06.getSigndate();
                        String key_id = day_id + "#" + manage_code;
                        ApplicationGeneralResultWithColumnName applicationGeneralResult = new ApplicationGeneralResultWithColumnName();
                        applicationGeneralResult.setManage_name(manage_name);
                        applicationGeneralResult.setManage_code(manage_code);
                        applicationGeneralResult.setDay_id(day_id);
                        applicationGeneralResult.setKey_id(key_id);
                        applicationGeneralResult.setApprove_num_day(Double.valueOf(lbpolKafka06.getPrem()));

                        applicationGeneralResult.setColumnName("approve_num_day;approve_num_day;key_id,day_id,manage_code,manage_name");

                        //正对decimal类型得处理，不能是null需要专门给0.0
                        return applicationGeneralResult;
                    }
                });
        source.addSink(new InsertHbaseOnlyV2LB("KLMIDAPPRUN:AppGeneralResultLC","APPLICATION_GENERAL_RESULT_RT"));

    }
}
