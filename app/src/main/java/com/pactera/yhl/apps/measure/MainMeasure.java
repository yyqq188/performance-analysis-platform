package com.pactera.yhl.apps.measure;

import com.pactera.yhl.apps.measure.aggregate.Calculation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/11 13:32
 */
public class MainMeasure {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(parameterTool);

        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));

        //-------------------------------------------------
        //ASSESSMENT_RATE_CONFIG
//        JobMeasure.midAssessment_rate_config(env,"assessment_rate_config",kafkaProp,
//                "KLMIDAPP:ASSESSMENT_RATE_CONFIG",
//                new String[]{"product_code","pay_period"},
//                new String[]{"key_id"},
//                "assessment_rate_config");

        //中间表
        //lbpol
//        JobMeasure.midLbpol(env,"suntestlbp",kafkaProp,
//                "KLMIDAPP:LBPOL_CONTNO",
//                new String[]{"contno"},
//                new String[]{"polno"},
//                "");
//        //lbcont
//        JobMeasure.midLbcont(env,"suntestlbcont",kafkaProp,
//                "KLMIDAPP:LBCONT_CONTNO",
//                new String[]{"contno"},
//                new String[]{"contno"},
//                "");
//        //-------------------------------------------------
//        //lpedoritem
//        JobMeasure.midlpedoritem(env,"suntestlpd",kafkaProp, "KLMIDAPP:LPEDORITEM_CONTNO_EDORNO",
//                new String[]{"contno","edorno"},
//                new String[]{"edorno"},
//                "");

//        JobMeasure.midT02salesinfo(env,"suntestsalesT",kafkaProp,
//                "KLMIDAPP:T02SALESINFO_K_SALES_ID",
//                new String[]{"sales_id"},
//                new String[]{"sales_id"},
//                "");

        //-------------------------------------------------
        //lcpol
//        JobMeasure.midLcpol(env,"suntestlcp",kafkaProp,
//                "KLMIDAPP:LCPOL_CONTNO",
//                new String[]{"contno"},
//                new String[]{"polno"},
//                "");
//
//        //lccont
//        JobMeasure.midLccont(env,"suntestlccont",kafkaProp,
//                "KLMIDAPP:LCCONT_CONTNO",
//                new String[]{"contno"},
//                new String[]{"contno"},
//                "");


        //-------------------------------------------------
        //T01TEAMINFO_TEAM_ID
//        JobMeasure.midT01teaminfo(env,"suntestteam",kafkaProp,
//                "KLMIDAPP:T01TEAMINFO_TEAM_ID",
//                new String[]{"team_id"},
//                new String[]{"team_id"},
//                "");

//        JobMeasure.midT02salesinfo(env,"suntestsalesT",kafkaProp,
//                "KLMIDAPP:T02SALESINFOK_TEAMID",
//                new String[]{"team_id"},
//                new String[]{"sales_id"},
//                "");


        //Todo T02SALESINFOK_LEADER_ID  人员拉平表
//        JobMeasure.midT02salesinfoFlat(env,"suntestsalesT",kafkaProp);

        //KLMIDAPP:T01BRANCHINFO_BRANCH_ID
//        JobMeasure.midT01branchinfo(env,"suntestbranch",kafkaProp,
//                "KLMIDAPP:T01BRANCHINFO_BRANCH_ID",
//                new String[]{"branch_id"},
//                new String[]{"branch_id"},
//                "");



        //-------------------------------------------------
        //关联层
        //Todo lbpol    suntestlbp      lbp2lpd
        JobMeasure.lbpol2lpedoritem(env,"suntestlbp",kafkaProp,"lbp2lpd");
        //Todo      lbp2lpd             lpd2lbp
        JobMeasure.lpedoritem2lbpol(env,"lbp2lpd",kafkaProp,"lpd2lbp");
        //Todo      lpd2lbp             lbp2product
        JobMeasure.lbpol2product(env,"lpd2lbp",kafkaProp,"lbp2product");
        //Todo      lbp2product         product2lbcont
        JobMeasure.product2lbcont(env,"lbp2product",kafkaProp,"product2lbcont");
        //Todo      product2lbcont      lcont2rate
        JobMeasure.lbcont2rate(env,"product2lbcont",kafkaProp,"lcont2rate");//与lccont公用

        //-------------------------------------------------

        //Todo lcpol   suntestlcp       lcp2lcp
        JobMeasure.lcpol2lcpol(env,"suntestlcp",kafkaProp,"lcp2lcp");
        //Todo      lcp2lcp             lcp2product
        JobMeasure.lcpol2product(env,"lcp2lcp",kafkaProp,"lcp2product");
        //Todo      lcp2product         product2lccont
        JobMeasure.product2lccont(env,"lcp2product",kafkaProp,"product2lccont");
        //Todo      product2lccont      lcont2rate
        JobMeasure.lccont2rate(env,"product2lccont",kafkaProp,"lcont2rate");//与lbcont公用 topic

        //-------------------------------------------------
        //Todo      lcont2rate          rate2sales
        JobMeasure.rate2salesinfo(env,"lcont2rate",kafkaProp,"rate2sales");
        //Todo      rate2sales          sales2team
        JobMeasure.salesinfo2bteaminfo(env,"rate2sales",kafkaProp,"sales2team");
        //Todo      sales2team          team2sales
        JobMeasure.teaminfo2salesinfo(env,"sales2team",kafkaProp,"team2sales");
        //Todo      team2sales          sales2branch
        JobMeasure.salesinfo2branchinfo(env,"team2sales",kafkaProp,"sales2branch");
        //Todo      sales2branch        aggregate
        JobMeasure.branchinfo2aggregate(env,"sales2branch",kafkaProp,"aggregate");

        //-------------------------------------------------
        //Todo 计算层  aggregate
        Calculation.calculatePremium(env,"aggregate",kafkaProp);
        env.execute("");
    }

}
