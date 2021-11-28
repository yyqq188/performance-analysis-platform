package com.pactera.yhl;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(params);

        //获得kafka的配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));
        String topic = "listables";
//        String topic = "testyhlv2";



        Job.insertHbaseTable2(env,topic,props, Config.anychatcont_ins,  Config.anychatcont_rowkeys, Config.anychatcont_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.labranchgroup_ins,  Config.labranchgroup_rowkeys, Config.labranchgroup_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.lbpol_ins,  Config.lbpol_rowkeys, Config.lbpol_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.lcaddress_ins,  Config.lcaddress_rowkeys, Config.lcaddress_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.lcappnt_ins,  Config.lcappnt_rowkeys, Config.lcappnt_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.lccont_ins,  Config.lccont_rowkeys, Config.lccont_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.lccontextend_ins,  Config.lccontextend_rowkeys, Config.lccontextend_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.lcinsured_ins,  Config.lcinsured_rowkeys, Config.lcinsured_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.lcphoinfonewresult_ins,  Config.lcphoinfonewresult_rowkeys, Config.lcphoinfonewresult_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.ldcode_ins,  Config.ldcode_rowkeys, Config.ldcode_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.ldcom_ins,  Config.ldcom_rowkeys, Config.ldcom_columnNames, "");
//        Job.insertHbaseTable2(env,topic,props, Config.lmedoritem_ins,  Config.lmedoritem_rowkeys, Config.lmedoritem_columnNames, "");





        env.execute(Main.class.getSimpleName());

    }
}
