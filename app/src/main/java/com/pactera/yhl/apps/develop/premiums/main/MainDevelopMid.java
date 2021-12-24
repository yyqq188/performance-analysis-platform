package com.pactera.yhl.apps.develop.premiums.main;

import com.pactera.yhl.apps.develop.premiums.job.JobPremiums;
import com.pactera.yhl.entity.source.Lcpol;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MainDevelopMid {
    public static void main(String[] args) throws Exception{
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

        String topic = "testyhlv22LB";
        //中间表
//        JobPremiums.midLcpol(env,topic,kafkaProp,
//                "KLMIDAPP:lcpol_agentcode",
//                new String[]{"agentcode"},
//                new String[]{"polno"},
//                "lcpol");
//
//        JobPremiums.midSaleinfoK(env,topic,kafkaProp,
//                "KLMIDAPP:t02salesinfok_salesId",
//                new String[]{"sales_id"},
//                new String[]{},
//                "t02salesinfok");
//
//        JobPremiums.midT01branchinfo(env,topic,kafkaProp,
//                "KLMIDAPP:t01branchinfo_branchid",
//                new String[]{"branch_id"},
//                new String[]{},
//                "t01branchinfo");
//
//        JobPremiums.midLpedoritem(env,topic,kafkaProp,
//                "KLMIDAPP:lpedoritem_contno_edorno",
//                new String[]{"contno","edorno"},
//                new String[]{"contno","edoracceptno","edorno","edortype","insuredno","polno"},
//                "lpedoritem");
//
//        JobPremiums.midLbpol2(env,topic,kafkaProp,
//                "KLMIDAPP:lbpol_contno_edorno",
//                new String[]{"contno","edorno"},
//                new String[]{"polno"},
//                "lbpol");
//
//        JobPremiums.midLbpol(env,topic,kafkaProp,
//                "KLMIDAPP:lbpol_agentcode",
//                new String[]{"agentcode"},
//                new String[]{"polno"},
//                "lbpol");
//
//        //自关联的话，要尽量考虑列要小，所以就只要表名做列名就可,rowkey就是源表的主键
//        JobPremiums.midLcpolOrder(env,topic,kafkaProp,
//                "KLMIDAPP:lcpol_payendyear_insuyear",
//                new String[]{"polno"},
//                new String[]{},
//                new String[]{"payendyear","insuyear"},   //需要更新的列
//                "desc",        //升序还是降序
//                Lcpol.class,  // hbase数据的类型
//                "lcpol");
//
//
//        JobPremiums.midProductRateConfig(env,topic,kafkaProp,
//                "KLMIDAPP:productrateconfig_keyid",
//                new String[]{"product_code","pay_period"},
//                new String[]{"start_date"},
//                "productrateconfig");
//
//


//        JobPremiums.midLbpolOrder(env,topic,kafkaProp,
//        "KLMIDAPP:lbpol_payendyear_insuyear",
//        new String[]{"polno"},
//        new String[]{},
//        new String[]{"payendyear","insuyear"},   //需要更新的列
//        "desc",        //升序还是降序
//        Lcpol.class,  // hbase数据的类型
//        "lbpol");

//
//        //缴费类型
//        JobPremiums.midPeriodType(env,topic,kafkaProp,
//                "KLMIDAPP:product_config_period_type",
//                new String[]{"product_code"},
//                new String[]{},
//                "product_config");


////////////////////////////////////////////////////


//        JobPremiums.midLcpol2Lcpol(env,topic,kafkaProp,
//                "KLMIDAPP:lcpol_contno",
//                new String[]{"contno"},
//                new String[]{},
//                "lcpol_contno");


        env.execute("mid");

    }
}
