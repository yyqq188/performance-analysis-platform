package com.pactera.yhl.apps.develop.premiums.main;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.*;
import com.pactera.yhl.apps.develop.premiums.job.JobPremiums;
import com.pactera.yhl.apps.develop.premiums.main.job.lb.AppGeneralResultLBV4;
import com.pactera.yhl.apps.develop.premiums.main.job.lb.AppProductDetailLBV4;
import com.pactera.yhl.apps.develop.premiums.main.job.lb.AppProductResultLBV4;
import com.pactera.yhl.apps.develop.premiums.sink.InsertKafkaOnly;
import com.pactera.yhl.entity.source.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * String tableName = "KLMIDAPP:lbpol_agentcode";//HBase中间表名
 * String[] rowkeys = new String[]{"agentcode"};
 * String[] columnNames = new String[]{"polno"};
 * String columnTableName = "lbpol";
 */
public class MainDevelopStreamTwoWithPeriod {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(6)));
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(parameterTool);

        parameterTool.getProperties();
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));
        String topic = "testyhlv2";
        String lcTopic = "testyhlv3";
        String lbTopic = "testyhlv4";

        //todo 这里改为了left join
        JobPremiums.lbpol2lpedoritem(
                env,
                "testyhlv2LB",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv5LB",// 输出topic
                "KLMIDAPP:lpedoritem_contno_edorno",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("contno","contno");
                        put("edorno","edorno");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom",
                        "polno","payyears","signdate","amnt","edorno","contno")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("modifydate","edortype","edorstate")),// 要从hbase中取得的字段
                Lpedoritem.class,// hbase表的实体类名字
                LbpolKafka01.class,// 发到kafka的实体类的名字  (固定的变量名)
                "KLMIDAPPRUN:LbpolKafka01",  //输出的hbase表名
                new LinkedHashMap<String,String>(){
                    {
                        put("agentcode","agentcode");
                    }
                },
                new HashMap<>(), //过滤driver的字段和值
//                new HashMap<String,String>(){
//                    {
//                        put("modifydate","function_date");
//                        put("key_value","edorstate=0,edortype=WT");
//                    }
//                }
                new HashMap<>()
                );


        //又加了一层过滤  edorno,signdate

        kafkaProp.setProperty("group.id","JobPremiums_lbpol2lpedoritem2");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "testyhlv5LB", new SimpleStringSchema(), kafkaProp);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka01>() {
                    @Override
                    public LbpolKafka01 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka01.class);
                    }
                }).filter(new FilterFunction<LbpolKafka01>() {
                    @Override
                    public boolean filter(LbpolKafka01 lbpolKafka01) throws Exception {
////                        String todayStr = new SimpleDateFormat("yyyy-MM-dd")
////                        .format(new Date(System.currentTimeMillis()));
//                        String signData = lbpolKafka01.getSigndate().split("\\s+")[0];
//                        String modifyDate = lbpolKafka01.getModifydate().split("\\s+")[0];
//                        String todayStr = "2021-09-30";
//                        String edorState = lbpolKafka01.getEdorstate().toString();
//                        String edorType = lbpolKafka01.getEdortype().toString();
//                        String edorNo = lbpolKafka01.getEdorno().toString();
//                        if((todayStr.equals(signData) && edorNo.startsWith("YBT")) ||
//                        (todayStr.equals(modifyDate) && edorState.equals("0") && edorType.equals("WT"))){
//                            return true;
//                        }else{
//                            return false;
//                        }


                        String todayStr = new SimpleDateFormat("yyyy-MM-dd")
                        .format(new Date(System.currentTimeMillis()));
//                        String todayStr = "2021-09-30";
                        String signData = lbpolKafka01.getSigndate().split("\\s+")[0];
                        String edorNo = lbpolKafka01.getEdorno().toString();
                        if((todayStr.equals(signData) && edorNo.startsWith("YBT"))){
                            return true;
                        }else{
                            if(!Objects.isNull(lbpolKafka01.getModifydate())){
                                String modifyDate = lbpolKafka01.getModifydate().split("\\s+")[0];
                                String edorState = lbpolKafka01.getEdorstate().toString();
                                String edorType = lbpolKafka01.getEdortype().toString();
                                if((todayStr.equals(modifyDate)
                                        && edorState.equals("0")
                                        && edorType.equals("WT"))){
                                    return true;
                                }else{
                                    return false;
                                }
                            }else{
                                return false;
                            }

                        }


                    }
                }).addSink(new InsertKafkaOnly<>("testyhlv6LB"));












        //1
//        JobPremiums.lpedoritem2lbpol(
//                env,
//                "testyhlv2LB",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv5LB",// 输出topic
//                "KLMIDAPP:lbpol_contno_edorno",// 需要关联的中间hbase表
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("contno","contno");
//                        put("edorno","edorno");
//                    }
//                },// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("modifydate","edortype","edorstate")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom",
//                        "polno","payendyear","signdate","amnt","contno")),// 要从hbase中取得的字段
//                Lbpol.class,// hbase表的实体类名字
//                LbpolKafka01.class,// 发到kafka的实体类的名字  (固定的变量名)
//                "KLMIDAPPRUN:LbpolKafka01",  //输出的hbase表名
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("agentcode","agentcode");
//                    }
//                }, //输出hbase表的rowkey
//                new HashMap<>(),// 过滤driver的字段和值
////                new HashMap<String,String>(){
////                    {
////                        put("edorvalidate","function_date");
////                        put("key_value","edorstate=0,edortype=WT");
////
////                    }
////                }
//                new HashMap<>()
//                ); //过滤hbase的字段和值

        //2
        JobPremiums.lbpolKafka01Tosaleinfo(
                env,
                "testyhlv6LB",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv7LB",// 输出topic
                "KLMIDAPP:t02salesinfok_salesId",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("agentcode","agentcode");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("agentcode","managecom",
                        "prem","agentcom","modifydate","edortype","edorstate",
                        "polno","payyears","signdate","amnt","contno")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("workarea","channel_id")),// 要从hbase中取得的字段 area_type
                T02salesinfok.class,// hbase表的实体类名字
                LbpolKafka02.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()
//                new HashMap<String,String>(){
//                    {
//                        put("key_value","channel_id=08");
//                    }
//                }
        ); //过滤的字段和值
//        JobPremiums.saleinfoTolbpolKafka01(
//                env,
//                "testyhlv2LB",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv6LB",// 输出topic
//                "KLMIDAPPRUN:LbpolKafka01",// 需要关联的中间hbase表
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("sales_id","sales_id");
//                    }
//                },// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("workarea","channel_id")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom",
//                        "modifydate","edortype","edorstate",
//                        "polno","payendyear","signdate","amnt","contno")),// 要从hbase中取得的字段
//                LbpolKafka01.class,// hbase表的实体类名字
//                LbpolKafka02.class,// 发到kafka的实体类的名字  (固定的变量名)
//                new HashMap<>(),
//                new HashMap<>());
//
        //3
        JobPremiums.lbpolKafka02ToBranchinfo(
                env,
                "testyhlv7LB",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv8LB",// 输出topic
                "KLMIDAPP:t01branchinfo_branchid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("managecom","managecom");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","agentcode","managecom","prem",
                        "modifydate","edortype","edorstate",
                        "polno","payyears","signdate","amnt","contno")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("branch_id","branch_name",
                        "class_id","branch_id_parent","branch_id_full")),// 要从hbase中取得的字段
                T01branchinfo.class,// hbase表的实体类名字
                LbpolKafka03.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值


          //4
        JobPremiums.ComplexLogicProcessLB(
                env,
                "testyhlv8LC",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv9LC",// 输出topic
                "KLMIDAPP:product_config_period_type",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("polno","polno");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "branch_name","branch_id","class_id","branch_id_parent","branch_id_full",
                        "polno","signdate","amnt","contno",
                        "contplancode","mainpolno","payintv","riskcode")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("contplancode","riskcode","payyears")),// 要从hbase中取得的字段
                Lcpol.class,// hbase表的实体类名字
                LbpolKafka04.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值

          //5
        JobPremiums.lbpolToProductRateConfig(
                env,
                "testyhlv9LB",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv10LB",// 输出topic
                "KLMIDAPP:productrateconfig_keyid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("contplancode","contplancode");
                        put("riskcode","riskcode");
                        put("payyears","payyears");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "branch_name","branch_id","modifydate",
                        "class_id","branch_id_parent","branch_id_full",
                        "polno","payyears","signdate","amnt",
                        "contplancode","riskcode","edorstate","edortype","contno")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("product_name","rate","start_date",
                        "end_date","period_type","state","pay_period")),// 要从hbase中取得的字段
                ProductRateConfig.class,// hbase表的实体类名字
                LbpolKafka05.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值


        //有的数据里没有期缴趸交的字段，以及产品名称 为了补充上
        JobPremiums.lbpolToProductConfig(
                env,
                "testyhlv10LB",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv11LB",// 输出topic
                "KLMIDAPP:product_config_period_type",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("contplancode","contplancode");
                        put("riskcode","riskcode");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "branch_name","branch_id","modifydate",
                        "class_id","branch_id_parent","branch_id_full",
                        "polno","payyears","signdate","amnt",
                        "contplancode","riskcode","edorstate","edortype",
                        "rate","start_date",
                        "end_date","state","pay_period","contno")),//period_type product_name      要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("product_name","product_payintv")),// 要从hbase中取得的字段
                ProductConfig.class,// hbase表的实体类名字
                LbpolKafka06.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值


        String appGeneralResult = "APPLICATION_GENERAL_RESULT_RT";
        String appProductDetail = "APPLICATION_PRODUCT_DETIAL_RT";
        String appProductResult = "APPLICATION_PRODUCT_RESULT_RT";

//        AppGeneralResultLBV4.LB_branch_name(env,"testyhlv11LB",kafkaProp,"",appGeneralResult);
//        AppGeneralResultLBV4.LB_all(env,"testyhlv11LB",kafkaProp,"",appGeneralResult);
//        AppGeneralResultLBV4.LB_branch_name_periodtype(env,"testyhlv11LB",kafkaProp,"",appGeneralResult);
//        AppGeneralResultLBV4.LB_all_periodtype(env,"testyhlv11LB",kafkaProp,"",appGeneralResult);
//
//        AppGeneralResultLBV4.LB_branch_name_num(env,"testyhlv11LB",kafkaProp,"",appGeneralResult);
//        AppGeneralResultLBV4.LB_all_num(env,"testyhlv11LB",kafkaProp,"",appGeneralResult);
//
//        /////
//        AppProductResultLBV4.LB_branch_name_product(env,"testyhlv11LB",kafkaProp,"",appProductResult);
//        AppProductResultLBV4.LB_branch_name_num_product(env,"testyhlv11LB",kafkaProp,"",appProductResult);
//        AppProductResultLBV4.LB_all_num_product(env,"testyhlv11LB",kafkaProp,"",appProductResult);
//        /////
//        AppProductDetailLBV4.LB_branch_name_product(env,"testyhlv11LB",kafkaProp,"",appProductDetail);
//        AppProductDetailLBV4.LB_branch_name_product_payperiod_num(env,"testyhlv11LB",kafkaProp,"",appProductDetail);
        AppProductDetailLBV4.LB_branch_name_product_payperiod(env,"testyhlv11LB",kafkaProp,"",appProductDetail);
//        AppProductDetailLBV4.LB_all_product_payperiod(env,"testyhlv11LB",kafkaProp,"",appProductDetail);
        env.execute("two");
    }
}
