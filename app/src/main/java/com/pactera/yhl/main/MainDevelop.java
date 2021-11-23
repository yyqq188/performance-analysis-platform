package com.pactera.yhl.main;

import com.pactera.yhl.apps.develop.premiums.JobPremiums;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Properties;

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
        String lbTopic = "testyhlv4";
        String lcTopic = "testyhlv3";
        //premiums
        //中间表

//        JobPremiums.midLbpol(env,topic,kafkaProp,
//                "KLMIDAPP:lbpol_agentcode",
//                new String[]{"agentcode"},
//                new String[]{"polno"},
//                "lbpol");
//        JobPremiums.midLcpol(env,topic,kafkaProp,
//                "KLMIDAPP:lcpol_agentcode",
//                new String[]{"agentcode"},
//                new String[]{"polno"},
//                "lcpol");
//        JobPremiums.midSaleinfoK(env,topic,kafkaProp,
//                "KLMIDAPP:t02salesinfok_salesId",
//                new String[]{"sales_id"},
//                new String[]{},
//                "t02salesinfok");

        //关联层
//        JobPremiums.lbpol2saleinfo(env,topic,kafkaProp,lbTopic);
//        JobPremiums.lcpol2saleinfo(env,topic,kafkaProp,lcTopic);
//        JobPremiums.saleinfo2lbpol(env,topic,kafkaProp,lbTopic);
//        JobPremiums.saleinfo2lcpol(env,topic,kafkaProp,lcTopic);

//        JobPremiums.premiums(env,topic,kafkaProp);




        JobPremiums.testCK(env,topic,kafkaProp);
        env.execute("");
    }
}
