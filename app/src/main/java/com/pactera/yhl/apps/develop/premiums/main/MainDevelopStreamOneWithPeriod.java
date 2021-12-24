package com.pactera.yhl.apps.develop.premiums.main;

import com.pactera.yhl.apps.develop.premiums.entity.*;
import com.pactera.yhl.apps.develop.premiums.job.ComplexLogic;
import com.pactera.yhl.apps.develop.premiums.job.JobPremiums;
import com.pactera.yhl.apps.develop.premiums.main.job.lc.AppGeneralResultLCV4;
import com.pactera.yhl.apps.develop.premiums.main.job.lc.AppProductDetailLCV4;
import com.pactera.yhl.apps.develop.premiums.main.job.lc.AppProductResultLCV4;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.entity.source.ProductConfig;
import com.pactera.yhl.entity.source.T01branchinfo;
import com.pactera.yhl.entity.source.T02salesinfok;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.kafka.clients.producer.Producer;

import java.util.*;

public class MainDevelopStreamOneWithPeriod {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(parameterTool);

        parameterTool.getProperties();
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));
        JobPremiums.lcpol2saleinfo(
                env,
                "listables",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv3LC",// 输出topic
                "KLMIDAPP:t02salesinfok_salesId",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("agentcode","agentcode");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("managecom","prem","agentcom","polno","payyears",
                        "signdate","amnt","contno","contplancode","mainpolno","payintv","riskcode")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("workarea","channel_id")),// 要从hbase中取得的字段
                T02salesinfok.class,// hbase表的实体类名字
                PremiumsKafkaEntity01.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),//过滤driver的字段和值
                new HashMap<String,String>(){
                    {
                        put("key_value","channel_id=08");
                    }
                }
                ); //过滤hbase的字段和值


//        JobPremiums.saleinfo2lcpol(
//                env,
//                "testyhlv2LC",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv3LC",// 输出topic
//                "KLMIDAPP:lcpol_agentcode",// 需要关联的中间hbase表
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("sales_id","sales_id");
//                    }
//                },// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("workarea","channel_id")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("managecom","prem","agentcom","polno","payyears","signdate","amnt","contno")),// 要从hbase中取得的字段
//                Lcpol.class,// hbase表的实体类名字
//                PremiumsKafkaEntity01.class,// 发到kafka的实体类的名字  (固定的变量名)
//                new HashMap<String,String>(){
//                    {
//                        put("channel_id","08");
//                    }
//                },//过滤driver的字段和值
//                new HashMap<>()
//        ); //过滤hbase的字段和值


        JobPremiums.PremiumsKafkaEntity01ToBranchId(
                env,
                "testyhlv3LC",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv4LC",// 输出topic
                "KLMIDAPP:t01branchinfo_branchid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("managecom","managecom");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "polno","payyears","signdate","amnt","contno",
                        "contplancode","mainpolno","payintv","riskcode")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("branch_name","branch_id",
                        "class_id","branch_id_parent","branch_id_full")),// 要从hbase中取得的字段
                T01branchinfo.class,// hbase表的实体类名字
                PremiumsKafkaEntity02.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值



        JobPremiums.PremiumsKafkaEntity01ToBranchId2(
                env,
                "testyhlv4LC",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv5LC",// 输出topic
                "KLMIDAPP:t01branchinfo_branchid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("branch_id_full","branch_id_full");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom",
                        "agentcom","channel_id","polno","payyears",
                        "signdate","amnt","contno",
                        "contplancode","mainpolno","payintv","riskcode")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("branch_name","branch_id",
                        "class_id","branch_id_parent","branch_id_full")),// 要从hbase中取得的字段
                T01branchinfo.class,// hbase表的实体类名字
                PremiumsKafkaEntity02.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值


//        JobPremiums.lcpolTolcpol(
//                env,
//                "testyhlv5LC",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv6LC",// 输出topic
//                "KLMIDAPP:LCPOL_CONTNO",// 需要关联的中间hbase表
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("polno","polno");
//                    }
//                },// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
//                        "branch_name","branch_id","class_id","branch_id_parent","branch_id_full",
//                        "polno","signdate","amnt","contno")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("contplancode","riskcode","payyears")),// 要从hbase中取得的字段
//                Lcpol.class,// hbase表的实体类名字
//                PremiumsKafkaEntity03.class,// 发到kafka的实体类的名字  (固定的变量名)
//                new HashMap<>(),
//                new HashMap<>()); //过滤的字段和值

        JobPremiums.ComplexLogicProcess(
                env,
                "testyhlv5LC",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv6LC",// 输出topic
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
                PremiumsKafkaEntity03.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值


        //这里是外关联
        JobPremiums.lcpolToProductRateConfig(
                env,
                "testyhlv6LC",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv7LC",// 输出topic
                "KLMIDAPP:productrateconfig_keyid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("contplancode","contplancode");
                        put("riskcode","riskcode");
                        put("payyears","payyears");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "branch_name","branch_id","class_id","branch_id_parent",
                        "branch_id_full","polno","payyears","signdate","amnt",
                        "contplancode","riskcode","contno")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("product_name","rate","start_date",
                        "end_date","period_type","state","pay_period")),// 要从hbase中取得的字段
                ProductRateConfig.class,// hbase表的实体类名字
                PremiumsKafkaEntity05.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值


        JobPremiums.lcpolToProductConfig(
                env,
                "testyhlv7LC",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv8LC",// 输出topic
                "KLMIDAPP:product_config_period_type",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("contplancode","contplancode");
                        put("riskcode","riskcode");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "branch_name","branch_id","class_id","branch_id_parent",
                        "branch_id_full","polno","payyears","signdate","amnt",
                        "contplancode","riskcode","rate","start_date",
                        "end_date","state","pay_period","contno")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("product_name","product_payintv")),// 要从hbase中取得的字段
                ProductConfig.class,// hbase表的实体类名字
                PremiumsKafkaEntity05.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值





        String appGeneralResult = "APPLICATION_GENERAL_RESULT_RT";
        String appProductDetail = "APPLICATION_PRODUCT_DETIAL_RT";
        String appProductResult = "APPLICATION_PRODUCT_RESULT_RT";



        //总公司


        //table1
//        AppGeneralResultLCV4.LC_branch_name(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//        AppGeneralResultLCV4.LC_workarea(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//        AppGeneralResultLCV4.LC_all(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//
//        AppGeneralResultLCV4.LC_branch_name_periodtype(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//        AppGeneralResultLCV4.LC_workarea_periodtype(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//        AppGeneralResultLCV4.LC_all_periodtype(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//
//        AppGeneralResultLCV4.LC_branch_name_num(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//        AppGeneralResultLCV4.LC_workarea_num(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//        AppGeneralResultLCV4.LC_all_num(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appGeneralResult);
//
//
//        //table2
//        AppProductResultLCV4.LC_branch_name_product(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductResult);
//        AppProductResultLCV4.LC_branch_name_product_num(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductResult);
//        AppProductResultLCV4.LC_all_product(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductResult);
//        AppProductResultLCV4.LC_all_product_num(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductResult);

        //table3
//        AppProductDetailLCV4.LC_branch_name_product(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductDetail);
        AppProductDetailLCV4.LC_branch_name_product_payperiod(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductDetail);
//        AppProductDetailLCV4.LC_all_product_payperiod(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductDetail);

        //这个待做
//        AppProductDetailLCV4.LC_branch_name_product_payperiod_num(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductDetail);
//        AppProductDetailLCV4.LC_all_product_payperiod_num(env,"testyhlv8LC",kafkaProp,"testyhlv9LC",appProductDetail);

        env.execute("complex");
    }
}
