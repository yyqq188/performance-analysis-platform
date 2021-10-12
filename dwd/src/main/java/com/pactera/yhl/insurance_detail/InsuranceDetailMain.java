package com.pactera.yhl.insurance_detail;

import com.pactera.yhl.insurance_detail.job.tmp_job.JobLuDefine;
import com.pactera.yhl.insurance_detail.sink.TmpLupolSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class InsuranceDetailMain {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        //lu生成表
        JobLuDefine.lupol_from_lbpol(env, kafkaProp, topic,"");
        JobLuDefine.lupol_from_lcpol(env, kafkaProp, topic,"");
//        JobLuDefine.lucont_from_lbcont(env, kafkaProp, topic,"");
//        JobLuDefine.lucont_from_lccont(env, kafkaProp, topic,"");

//        //中间表
//        JobMidDefine.jobTable1(env, new String[]{params.get("databaselist")},new String[]{params.get("tablelist1")});
//        JobMidDefine.jobTable23(env, new String[]{params.get("databaselist")},new String[]{params.get("tablelist23")});
//        env.execute(MidMain.class.getName());

//        //中间表
//        JobDefine.jobTable1(env, AgentDWDAllConfig.DATABASELIST,AgentDWDAllConfig.TABLELIST1);
//        JobDefine.jobTable23(env, AgentDWDAllConfig.DATABASELIST,AgentDWDAllConfig.TABLELIST23);
//        //关联job
//        JobDefine.jobTable1_23(env, AgentDWDAllConfig.DATABASELIST,AgentDWDAllConfig.TABLELIST1);
//        JobDefine.jobTable23_1(env, AgentDWDAllConfig.DATABASELIST,AgentDWDAllConfig.TABLELIST23);
//        env.execute(Main.class.getName());

//        Class<?> aClass = Class.forName("com.pactera.yhl.insurance_detail.sink.TmpLupolSink");
//        TmpLupolSink o =(TmpLupolSink) aClass.newInstance();
//        o.setTableName("aaa");
//        o.handle(null, null,null);

    }
}
