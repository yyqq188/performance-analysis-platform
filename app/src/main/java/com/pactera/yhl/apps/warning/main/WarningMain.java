package com.pactera.yhl.apps.warning.main;

/*import com.pactera.yhl.apps.develop.warning.config.MyConfig;
import com.pactera.yhl.apps.develop.warning.constant.TopicConstant;
import com.pactera.yhl.apps.develop.warning.job.JobDefine;*/
import com.pactera.yhl.apps.warning.config.MyConfig;
import com.pactera.yhl.apps.warning.constant.TopicConstant;
import com.pactera.yhl.apps.warning.job.JobDefine;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author SUN KI
 * @time 2021/11/15 19:06
 * @Desc
 */
public class WarningMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", MyConfig.KAFKAURL);
//        properties.setProperty("group.id", MyConfig.GROUPID);
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔
        properties.setProperty("auto.offset.reset", "earliest"); // latest/earliest

        String topic = TopicConstant.factwagebasetest;
        String groupIdKhrs = "khyjKhrs";
        String groupIdZj = "khyjZj";
        String groupIdBjl = "khyjBjl";
        String groupIdZy = "khyjZy";
        String groupIdZjfb = "khyjZjfb";
        String groupIdXzQbry = "khyjXzQbry";
        String groupIdXzZj = "khyjXzZj";
        String groupIdXzBjl = "khyjXzBjl";
        String groupIdXzZy = "khyjXzZy";
        //考核预警
//        JobDefine.jobGeneralResult_Khrs(env, topic, properties, groupIdKhrs);//考核人力
//        JobDefine.jobGeneralResult_KhrsTest(env, topic, properties, groupIdKhrs);//考核人力Test
//        JobDefine.jobGeneralResult_Zj(env, topic, properties, groupIdZj);//总监
//        JobDefine.jobGeneralResult_Bjl(env, topic, properties, groupIdBjl);//部经理
//        JobDefine.jobGeneralResult_Zy(env, topic, properties, groupIdZy);//专员
        //考核预警总监分布
//        JobDefine.jobDirectorResult_Zj(env,topic,properties,groupIdZjfb);
        //考核预警基本法
        JobDefine.jobAssessmentResult_Qbry(env,topic,properties,groupIdXzQbry);
//        JobDefine.jobAssessmentResult_Zj(env,topic,properties,groupIdXzZj);
//        JobDefine.jobAssessmentResult_Bjl(env,topic,properties,groupIdXzBjl);
//        JobDefine.jobAssessmentResult_Zy(env,topic,properties,groupIdXzZy);
        env.execute("WarningMain");
    }
}
