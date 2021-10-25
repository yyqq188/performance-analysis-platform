package com.pactera.yhl.f_agent;

import com.pactera.yhl.f_agent.job.JobDefine;
import com.pactera.yhl.f_agent.job.JobJoinDefine;
import com.pactera.yhl.f_agent.job.JobMidDefine;
import com.pactera.yhl.f_agent.sink.OXTO_Team_IdSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author: TSY
 * @create: 2021/10/20 0020 上午 10:04
 * @description:
 */
public class AgentDetailMain {
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


        //O_XG_T02SALESINFO_K生成表
        JobDefine.t02salesinfo_kInsert(env, kafkaProp, topic,"1");
        //JobLuPolDefine.lupol_from_lcpol(env, kafkaProp, topic,"bb");

        //生成中间层
        JobMidDefine.A_Branch_Id_ParentSink(env, kafkaProp, topic,"2");
        JobMidDefine.A_Branch_idSink(env, kafkaProp, topic,"3");
        JobMidDefine.OXTO_Branch_IdSink(env, kafkaProp, topic,"4");
        JobMidDefine.OXTT_Team_Id_ParentSink(env, kafkaProp, topic,"5");
        JobMidDefine.OXTT_Team_IdSink(env, kafkaProp, topic,"6");
        JobMidDefine.OXTO_Team_IdSink(env, kafkaProp, topic,"7");

        //Join
        JobJoinDefine.jobFAJoinATeam(env, kafkaProp, topic,"8");
        JobJoinDefine.jobFAJoinBranch(env, kafkaProp, topic,"9");
        JobJoinDefine.jobFAJoinFatherBranch(env, kafkaProp, topic,"10");
        JobJoinDefine.jobFAJoinGrandBranch(env, kafkaProp, topic,"11");
        JobJoinDefine.jobFAJoinPersonnel(env, kafkaProp, topic,"12");
        JobJoinDefine.jobFAJoinRTeam(env, kafkaProp, topic,"13");
        JobJoinDefine.jobFAJoinSonBranch(env, kafkaProp, topic,"14");
        JobJoinDefine.jobFAJoinTeam(env, kafkaProp, topic,"15");

    }
}
