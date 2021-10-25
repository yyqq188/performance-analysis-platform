package com.pactera.yhl.flinkck;

import com.pactera.yhl.flinkck.job.JobDefine;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class dwsMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);
        env.getConfig().setGlobalJobParameters(params);


        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));
        kafkaProp.setProperty("enable.auto.commit", params.toMap().get("kafka_enable_auto_commit"));
        kafkaProp.setProperty("auto.commit.interval.ms", params.toMap().get("kafka_auto_commit_interval"));//自动提交的时间间隔
        String topic = "listables";


//        //lupol生成表
        JobDefine.jobFAgentDefine(env,topic,kafkaProp);
        JobDefine.jobFPolicyDefine(env,topic,kafkaProp);
        env.execute(dwsMain.class.getSimpleName());
    }
}
