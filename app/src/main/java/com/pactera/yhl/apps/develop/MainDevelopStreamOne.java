package com.pactera.yhl.apps.develop;

import com.pactera.yhl.apps.develop.premiums.entity.*;
import com.pactera.yhl.apps.develop.premiums.job.JobPremiums;
import com.pactera.yhl.apps.develop.premiums.job.jobComputeV2.LCJob1;
import com.pactera.yhl.apps.develop.premiums.job.jobComputeV3.AppGeneralResultLC;
import com.pactera.yhl.apps.develop.premiums.job.jobComputeV3.AppProductDetailLC;
import com.pactera.yhl.apps.develop.premiums.job.jobComputeV3.AppProductResultLC;
import com.pactera.yhl.entity.source.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.App;

import java.util.*;

/**
 * String tableName = "KLMIDAPP:lbpol_agentcode";//HBase中间表名
 * String[] rowkeys = new String[]{"agentcode"};
 * String[] columnNames = new String[]{"polno"};
 * String columnTableName = "lbpol";
 */
public class MainDevelopStreamOne {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(parameterTool);

        parameterTool.getProperties();
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));

//        JobPremiums.lcpol2saleinfo(
//                env,
//                "testyhlv2",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv3",// 输出topic
//                "KLMIDAPP:t02salesinfok_salesId",// 需要关联的中间hbase表
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("agentcode","agentcode");
//                    }
//                },// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("managecom","prem","agentcom","polno","payendyear","signdate","amnt")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("workarea","channel_id")),// 要从hbase中取得的字段
//                T02salesinfok.class,// hbase表的实体类名字
//                PremiumsKafkaEntity01.class,// 发到kafka的实体类的名字  (固定的变量名)
//                new HashMap<>(),//过滤driver的字段和值
//                new HashMap<String,String>(){
//                    {
//                        put("key_value","channel_id=08");
//                    }
//                }
//                ); //过滤hbase的字段和值


        JobPremiums.saleinfo2lcpol(
                env,
                "testyhlv2",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv3",// 输出topic
                "KLMIDAPP:lcpol_agentcode",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("sales_id","sales_id");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","channel_id")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("managecom","prem","agentcom","polno","payendyear","signdate","amnt")),// 要从hbase中取得的字段
                Lcpol.class,// hbase表的实体类名字
                PremiumsKafkaEntity01.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<String,String>(){
                    {
                        put("channel_id","08");
                    }
                },//过滤driver的字段和值
                new HashMap<>()
                ); //过滤hbase的字段和值


        JobPremiums.PremiumsKafkaEntity01ToBranchId(
                env,
                "testyhlv3",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv4",// 输出topic
                "KLMIDAPP:t01branchinfo_branchid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("managecom","managecom");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id","polno","payendyear","signdate","amnt")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("branch_name","branch_id",
                        "class_id","branch_id_parent","branch_id_full")),// 要从hbase中取得的字段
                T01branchinfo.class,// hbase表的实体类名字
                PremiumsKafkaEntity02.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值



        JobPremiums.PremiumsKafkaEntity01ToBranchId2(
                env,
                "testyhlv4",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv5",// 输出topic
                "KLMIDAPP:t01branchinfo_branchid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("branch_id_full","branch_id_full");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id","polno","payendyear","signdate","amnt")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("branch_name","branch_id",
                        "class_id","branch_id_parent","branch_id_full")),// 要从hbase中取得的字段
                T01branchinfo.class,// hbase表的实体类名字
                PremiumsKafkaEntity02.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值

        JobPremiums.lcpolTolcpol(
                env,
                "testyhlv5",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv6",// 输出topic
                "KLMIDAPP:lcpol_payendyear_insuyear",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("polno","polno");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "branch_name","branch_id",
                        "class_id","branch_id_parent","branch_id_full","polno","payendyear","signdate","amnt")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("contplancode","riskcode")),// 要从hbase中取得的字段
                Lcpol.class,// hbase表的实体类名字
                PremiumsKafkaEntity03.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值

        JobPremiums.lcpolToProductRateConfig(
                env,
                "testyhlv6",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv7",// 输出topic
                "KLMIDAPP:productrateconfig_keyid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("contplancode","contplancode");
                        put("riskcode","riskcode");
                        put("payendyear","payendyear");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "branch_name","branch_id",
                        "class_id","branch_id_parent","branch_id_full","polno","payendyear","signdate","amnt",
                        "contplancode","riskcode")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("product_name","rate","start_date",
                        "end_date","period_type","state","pay_period")),// 要从hbase中取得的字段
                ProductRateConfig.class,// hbase表的实体类名字
                PremiumsKafkaEntity04.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值



        String appGeneralResult = "APPLICATION_GENERAL_RESULT_RT";
        String appProductDetail = "APPLICATION_PRODUCT_DETIAL_RT";
        String appProductResult = "APPLICATION_PRODUCT_RESULT_RT";

//        AppGeneralResultLC.LC_branch_name(env,"testyhlv7",kafkaProp,"testyhlv8",appGeneralResult);
//        AppGeneralResultLC.LC_workarea(env,"testyhlv7",kafkaProp,"testyhlv8",appGeneralResult);
//        AppGeneralResultLC.LC_branch_name_periodtype(env,"testyhlv7",kafkaProp,"testyhlv8",appGeneralResult);
//        AppGeneralResultLC.LC_workarea_periodtype(env,"testyhlv7",kafkaProp,"testyhlv8",appGeneralResult);

        AppProductResultLC.LC_branch_name(env,"testyhlv7",kafkaProp,"testyhlv8",appProductResult);
        AppProductDetailLC.LC_branch_name_product_payperiod(env,"testyhlv7",kafkaProp,"testyhlv8",appProductDetail);
        env.execute("one");
    }
}
