package com.pactera.yhl.apps.develop;

import com.pactera.yhl.apps.develop.premiums.entity.KafkaEntity;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity01;
import com.pactera.yhl.apps.develop.premiums.job.JobPremiums;
import com.pactera.yhl.entity.source.T02salesinfok;
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
        JobPremiums.midLcpol(env,topic,kafkaProp,
                "KLMIDAPP:lcpol_agentcode",
                new String[]{"agentcode"},
                new String[]{"polno"},
                "lcpol");


        JobPremiums.midSaleinfoK(env,topic,kafkaProp,
                "KLMIDAPP:t02salesinfok_salesId",
                new String[]{"sales_id"},
                new String[]{},
                "t02salesinfok");



//        JobPremiums.midLbpol(env,topic,kafkaProp,
//                "KLMIDAPP:lbpol_agentcode",
//                new String[]{"agentcode"},
//                new String[]{"polno"},
//                "lbpol");


//        JobPremiums.midLpedoritem(env,topic,kafkaProp,
//                "KLMIDAPP:lpedoritem_contno",
//                new String[]{"contno"},
//                new String[]{"contno","edoracceptno","edorno","edortype","insuredno","polno"},
//                "lpedoritem");
//
//        JobPremiums.midLbpol2(env,topic,kafkaProp,
//                "KLMIDAPP:lbpol_edorno",
//                new String[]{"edorno"},
//                new String[]{"polno"},
//                "lbpol");
//
//
//        JobPremiums.midT01branchinfo(env,topic,kafkaProp,
//                "KLMIDAPP:t01branchinfo_branchid",
//                new String[]{"branch_id"},
//                new String[]{},
//                "t01branchinfo");








        //关联层

        String tableName = "KLMIDAPP:t02salesinfok_salesId";//HBase中间表名
        Set<String> joinFieldsDriver = new HashSet<>();
        joinFieldsDriver.add("agentcode");
        Set<String> otherFieldsDriver = new HashSet<>();
        otherFieldsDriver.add("managecom");
        otherFieldsDriver.add("prem");
        Set<String> fieldsHbase = new HashSet<>();
        fieldsHbase.add("workarea");
        Class<?> hbaseClazz = T02salesinfok.class;
        Class<?> kafkaClazz = PremiumsKafkaEntity01.class;
        Map<String,String> filterMap = new HashMap<>();

        JobPremiums.lcpol2saleinfo(
                env,
                topic,  //输入topic
                kafkaProp,// kafka默认配置
                lcTopic,// 输出topic
                tableName,// 需要关联的中间hbase表
                joinFieldsDriver,// 要从主流中需要取得的字段 之 关联字段
                otherFieldsDriver,// 要从主流中需要取得的字段 之 其他字段
                fieldsHbase,// 要从hbase中取得的字段
                hbaseClazz,// hbase表的实体类名字
                kafkaClazz,// 发到kafka的实体类的名字  (固定的变量名)
                filterMap); //过滤的字段和值





//        JobPremiums.saleinfo2lcpol(env,topic,kafkaProp,lcTopic);
//
//        JobPremiums.saleinfo2lbpol(env,topic,kafkaProp,lbTopic);
//        JobPremiums.lbpol2saleinfo(env,topic,kafkaProp,lbTopic);











//        JobPremiums.premiums(env,topic,kafkaProp);




        JobPremiums.testCK(env,topic,kafkaProp);
        env.execute("");
    }
}
