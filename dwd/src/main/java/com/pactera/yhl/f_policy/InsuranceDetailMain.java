package com.pactera.yhl.f_policy;

import com.pactera.yhl.f_policy.job.tmp_job.JobMidLuContDefine;
import com.pactera.yhl.f_policy.job.tmp_job.JobLuPolDefine;
import com.pactera.yhl.f_policy.job.tmp_job.JobRowNumLjtempfeeclassDefine;
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


//        //lupol生成表
        JobLuPolDefine.lupol_from_lbpol(env, kafkaProp, topic,"aa");
        JobLuPolDefine.lupol_from_lcpol(env, kafkaProp, topic,"bb");

        //先生成lucont的中间层
        JobRowNumLjtempfeeclassDefine.lccont_rownum(env, kafkaProp, topic,"cc");
        JobMidLuContDefine.lucont_from_lbcont(env, kafkaProp, topic,"dd");
        JobMidLuContDefine.lucont_from_lccont(env, kafkaProp, topic,"ee");
        JobMidLuContDefine.lccont_rownum(env,kafkaProp,"topic11","ff");
        JobMidLuContDefine.lccont_infonewresult(env,kafkaProp,topic,"df");

        //再生成lucont表  join




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


        String t1= "20131011" ;
        String t2= "20131030" ;
        int  result = t1.compareTo(t2);

    }
}
