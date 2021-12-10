package com.pactera.yhl.apps.develop;

import com.pactera.yhl.apps.develop.premiums.entity.*;
import com.pactera.yhl.apps.develop.premiums.job.JobPremiums;
import com.pactera.yhl.apps.develop.premiums.job.jobComputeV2.LCJob1;
import com.pactera.yhl.entity.source.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

/**
 * String tableName = "KLMIDAPP:lbpol_agentcode";//HBase中间表名
 * String[] rowkeys = new String[]{"agentcode"};
 * String[] columnNames = new String[]{"polno"};
 * String columnTableName = "lbpol";
 */
public class MainDevelopStreamTwo {
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
        String topic = "testyhlv2";
        String lcTopic = "testyhlv3";
        String lbTopic = "testyhlv4";


//        JobPremiums.lbpol2lpedoritem(
//                env,
//                "testyhlv2",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv5",// 输出topic
//                "KLMIDAPP:lpedoritem_contno_edorno",// 需要关联的中间hbase表
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("contno","contno");
//                        put("edorno","edorno");
//                    }
//                },// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom",
//                        "polno","payendyear","signdate","amnt")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("edorvalidate","edortype","edorstate")),// 要从hbase中取得的字段
//                Lpedoritem.class,// hbase表的实体类名字
//                LbpolKafka01.class,// 发到kafka的实体类的名字  (固定的变量名)
//                "KLMIDAPPRUN:LbpolKafka01",  //输出的hbase表名
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("agentcode","agentcode");
//                    }
//                },
//                new HashMap<>(), //过滤driver的字段和值
////                new HashMap<String,String>(){
////                    {
////                        put("edorvalidate","function_date");
////                        put("key_value","edorstate=0,edortype=WT");
////                    }
////                }
//                new HashMap<>()
//                );


        //1
        JobPremiums.lpedoritem2lbpol(
                env,
                "testyhlv2",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv5",// 输出topic
                "KLMIDAPP:lbpol_contno_edorno",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("contno","contno");
                        put("edorno","edorno");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("edorvalidate","edortype","edorstate")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom",
                        "polno","payendyear","signdate","amnt")),// 要从hbase中取得的字段
                Lbpol.class,// hbase表的实体类名字
                LbpolKafka01.class,// 发到kafka的实体类的名字  (固定的变量名)
                "KLMIDAPPRUN:LbpolKafka01",  //输出的hbase表名
                new LinkedHashMap<String,String>(){
                    {
                        put("agentcode","agentcode");
                    }
                }, //输出hbase表的rowkey
                new HashMap<>(),// 过滤driver的字段和值
//                new HashMap<String,String>(){
//                    {
//                        put("edorvalidate","function_date");
//                        put("key_value","edorstate=0,edortype=WT");
//
//                    }
//                }
                new HashMap<>()
                ); //过滤hbase的字段和值

        //2
        JobPremiums.lbpolKafka01Tosaleinfo(
                env,
                "testyhlv5",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv6",// 输出topic
                "KLMIDAPP:t02salesinfok_salesId",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("agentcode","agentcode");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom","edorvalidate","edortype","edorstate",
                        "polno","payendyear","signdate","amnt")),// 要从主流中需要取得的字段 之 其他字段
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
//                "testyhlv2",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv6",// 输出topic
//                "KLMIDAPPRUN:LbpolKafka01",// 需要关联的中间hbase表
//                new LinkedHashMap<String,String>(){
//                    {
//                        put("sales_id","sales_id");
//                    }
//                },// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("workarea","channel_id")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom",
//                        "edorvalidate","edortype","edorstate",
//                        "polno","payendyear","signdate","amnt")),// 要从hbase中取得的字段
//                LbpolKafka01.class,// hbase表的实体类名字
//                LbpolKafka02.class,// 发到kafka的实体类的名字  (固定的变量名)
//                new HashMap<>(),
//                new HashMap<>());
//
        //3
        JobPremiums.lbpolKafka02ToBranchinfo(
                env,
                "testyhlv6",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv7",// 输出topic
                "KLMIDAPP:t01branchinfo_branchid",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("managecom","managecom");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","agentcode","managecom","prem",
                        "edorvalidate","edortype","edorstate",
                        "polno","payendyear","signdate","amnt")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("branch_id","branch_name",
                        "class_id","branch_id_parent","branch_id_full")),// 要从hbase中取得的字段
                T01branchinfo.class,// hbase表的实体类名字
                LbpolKafka03.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值


          //4
        JobPremiums.lbpolTolbpol(
                env,
                "testyhlv7",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv8",// 输出topic
                "KLMIDAPP:lbpol_payendyear_insuyear",// 需要关联的中间hbase表
                new LinkedHashMap<String,String>(){
                    {
                        put("polno","polno");
                    }
                },// 要从主流中需要取得的字段 之 关联字段
                new HashSet<>(Arrays.asList("workarea","prem","managecom","agentcom","channel_id",
                        "branch_name","branch_id",
                        "class_id","branch_id_parent","branch_id_full","polno","payendyear","signdate","amnt")),// 要从主流中需要取得的字段 之 其他字段
                new HashSet<>(Arrays.asList("contplancode","riskcode")),// 要从hbase中取得的字段
                Lbpol.class,// hbase表的实体类名字
                LbpolKafka04.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值
          //5
        JobPremiums.lbpolToProductRateConfig(
                env,
                "testyhlv8",  //输入topic
                kafkaProp,// kafka默认配置
                "testyhlv9",// 输出topic
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
                LbpolKafka05.class,// 发到kafka的实体类的名字  (固定的变量名)
                new HashMap<>(),
                new HashMap<>()); //过滤的字段和值










//        JobPremiums.premiums(env,topic,kafkaProp);

//        JobPremiums.testCK(env,topic,kafkaProp);




        //浙江
//        LCJob1.LC_branch_name(env,"testyhlv7",kafkaProp,"testyhlv8");
//        //宁波
//        LCJob1.LC_workarea(env,"testyhlv7",kafkaProp,"testyhlv8");
//        //浙江 期趸交
//        LCJob1.LC_branch_name_periodtype(env,"testyhlv7",kafkaProp,"testyhlv8");
//        //宁波 期趸交
//        LCJob1.LC_workarea_periodtype(env,"testyhlv7",kafkaProp,"testyhlv8");
//        //浙江 产品
//        LCJob1.LC_branch_name_product(env,"testyhlv7",kafkaProp,"testyhlv8");
//        //浙江 产品 年期
//        LCJob1.LC_branch_name_product_payperiod(env,"testyhlv7",kafkaProp,"testyhlv8");






        env.execute("two");
    }
}
