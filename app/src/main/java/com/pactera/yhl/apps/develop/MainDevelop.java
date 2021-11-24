package com.pactera.yhl.apps.develop;

import com.pactera.yhl.apps.develop.premiums.entity.*;
import com.pactera.yhl.apps.develop.premiums.job.JobPremiums;
import com.pactera.yhl.entity.source.*;
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
public class MainDevelop {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
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

        //premiums
        //中间表
//        JobPremiums.midLcpol(env,topic,kafkaProp,
//                "KLMIDAPP:lcpol_agentcode",
//                new String[]{"agentcode"},
//                new String[]{"polno"},
//                "lcpol");
//
//
//        JobPremiums.midSaleinfoK(env,topic,kafkaProp,
//                "KLMIDAPP:t02salesinfok_salesId",
//                new String[]{"sales_id"},
//                new String[]{},
//                "t02salesinfok");



        JobPremiums.midT01branchinfo(env,topic,kafkaProp,
                "KLMIDAPP:t01branchinfo_branchid",
                new String[]{"branch_id"},
                new String[]{},
                "t01branchinfo");






        JobPremiums.midLpedoritem(env,topic,kafkaProp,
                "KLMIDAPP:lpedoritem_contno_edorno",
                new String[]{"contno","edorno"},
                new String[]{"contno","edoracceptno","edorno","edortype","insuredno","polno"},
                "lpedoritem");
//
        JobPremiums.midLbpol2(env,topic,kafkaProp,
                "KLMIDAPP:lbpol_contno_edorno",
                new String[]{"contno","edorno"},
                new String[]{"polno"},
                "lbpol");

        JobPremiums.midLbpol(env,topic,kafkaProp,
                "KLMIDAPP:lbpol_agentcode",
                new String[]{"agentcode"},
                new String[]{"polno"},
                "lbpol");







        //关联层
//
//        String tableName = "KLMIDAPP:t02salesinfok_salesId";//HBase中间表名
//        Set<String> joinFieldsDriver = new HashSet<>();
//        joinFieldsDriver.add("agentcode");
//        Set<String> otherFieldsDriver = new HashSet<>();
//        otherFieldsDriver.add("managecom");
//        otherFieldsDriver.add("prem");
//        otherFieldsDriver.add("agentcom");
//        Set<String> fieldsHbase = new HashSet<>();
//        fieldsHbase.add("workarea");
//        Class<?> hbaseClazz = T02salesinfok.class;
//        Class<?> kafkaClazz = PremiumsKafkaEntity01.class;
//        Map<String,String> filterMap = new HashMap<>();
//
//        JobPremiums.lcpol2saleinfo(
//                env,
//                topic,  //输入topic
//                kafkaProp,// kafka默认配置
//                lcTopic,// 输出topic
//                tableName,// 需要关联的中间hbase表
//                joinFieldsDriver,// 要从主流中需要取得的字段 之 关联字段
//                otherFieldsDriver,// 要从主流中需要取得的字段 之 其他字段
//                fieldsHbase,// 要从hbase中取得的字段
//                hbaseClazz,// hbase表的实体类名字
//                kafkaClazz,// 发到kafka的实体类的名字  (固定的变量名)
//                filterMap); //过滤的字段和值
//
//
//        JobPremiums.PremiumsKafkaEntity01ToBranchId(
//                env,
//                "testyhlv2",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv3",// 输出topic
//                "KLMIDAPP:lcpol_agentcode",// 需要关联的中间hbase表
//                new HashSet<>(Arrays.asList("sales_id")),// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("workarea")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("managecom","prem","agentcom")),// 要从hbase中取得的字段
//                Lcpol.class,// hbase表的实体类名字
//                kafkaClazz,// 发到kafka的实体类的名字  (固定的变量名)
//                filterMap); //过滤的字段和值
//
//
//
//        JobPremiums.saleinfo2lcpol(
//                env,
//                "testyhlv3",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv4",// 输出topic
//                "KLMIDAPP:t01branchinfo_branchid",// 需要关联的中间hbase表
//                new HashSet<>(Arrays.asList("contno")),// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("workarea","prem","managecom")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("branch_name")),// 要从hbase中取得的字段
//                T01branchinfo.class,// hbase表的实体类名字
//                PremiumsKafkaEntity02.class,// 发到kafka的实体类的名字  (固定的变量名)
//                new HashMap<>()); //过滤的字段和值
//
//        ////////------------------------------------------------------------------
//        Map<String,String> lbpol2lpedoritemFilterMap = new HashMap<>();
//
//        lbpol2lpedoritemFilterMap.put("edorvalidate","function");
//        lbpol2lpedoritemFilterMap.put("edorstate","0");
//        lbpol2lpedoritemFilterMap.put("edortype","WT");
//
//        JobPremiums.lbpol2lpedoritem(
//                env,
//                "testyhlv2",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv5",// 输出topic
//                "KLMIDAPP:lpedoritem_contno_edorno",// 需要关联的中间hbase表
//                new HashSet<>(Arrays.asList("contno","edorno")),// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("edorvalidate","edortype","edorstate")),// 要从hbase中取得的字段
//                Lpedoritem.class,// hbase表的实体类名字
//                LbpolKafka01.class,// 发到kafka的实体类的名字  (固定的变量名)
//                lbpol2lpedoritemFilterMap); //过滤的字段和值
//
//
//
//        JobPremiums.lpedoritem2lbpol(
//                env,
//                "testyhlv2",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv5",// 输出topic
//                "KLMIDAPP:lbpol_contno_edorno",// 需要关联的中间hbase表
//                new HashSet<>(Arrays.asList("contno","edorno")),// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("edorvalidate","edortype","edorstate")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom")),// 要从hbase中取得的字段
//                Lpedoritem.class,// hbase表的实体类名字
//                LbpolKafka01.class,// 发到kafka的实体类的名字  (固定的变量名)
//                filterMap); //过滤的字段和值
//
//
//        JobPremiums.lbpolKafka01Tosaleinfo(
//                env,
//                "testyhlv5",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv6",// 输出topic
//                "KLMIDAPP:t02salesinfok_salesId",// 需要关联的中间hbase表
//                new HashSet<>(Arrays.asList("agentcode")),// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom","edorvalidate","edortype","edorstate")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("workarea")),// 要从hbase中取得的字段
//                Lbpol.class,// hbase表的实体类名字
//                LbpolKafka02.class,// 发到kafka的实体类的名字  (固定的变量名)
//                filterMap); //过滤的字段和值
//
//        JobPremiums.saleinfoTolbpolKafka01(
//                env,
//                "testyhlv5",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv6",// 输出topic
//                "KLMIDAPP:lbpol_agentcode",// 需要关联的中间hbase表
//                new HashSet<>(Arrays.asList("sales_id")),// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("workarea")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("agentcode","managecom","prem","agentcom","edorvalidate","edortype","edorstate")),// 要从hbase中取得的字段
//                T02salesinfok.class,// hbase表的实体类名字
//                LbpolKafka02.class,// 发到kafka的实体类的名字  (固定的变量名)
//                filterMap); //
//
//
//        JobPremiums.lbpolKafka02ToBranchinfo(
//                env,
//                "testyhlv6",  //输入topic
//                kafkaProp,// kafka默认配置
//                "testyhlv7",// 输出topic
//                "KLMIDAPP:t01branchinfo_branchid",// 需要关联的中间hbase表
//                new HashSet<>(Arrays.asList("agentcom")),// 要从主流中需要取得的字段 之 关联字段
//                new HashSet<>(Arrays.asList("workarea","agentcode","managecom","prem","edorvalidate","edortype","edorstate")),// 要从主流中需要取得的字段 之 其他字段
//                new HashSet<>(Arrays.asList("branch_id")),// 要从hbase中取得的字段
//                Lbpol.class,// hbase表的实体类名字
//                LbpolKafka03.class,// 发到kafka的实体类的名字  (固定的变量名)
//                filterMap); //过滤的字段和值





//        JobPremiums.premiums(env,topic,kafkaProp);

//        JobPremiums.testCK(env,topic,kafkaProp);
        env.execute("");
    }
}
