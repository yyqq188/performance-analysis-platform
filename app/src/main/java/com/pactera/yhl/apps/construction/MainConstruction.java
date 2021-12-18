package com.pactera.yhl.apps.construction;

import com.pactera.yhl.apps.construction.aggregate.ManpowerConstruction;
import com.pactera.yhl.apps.construction.aggregate.ManpowerJob;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author: TSY
 * @create: 2021/11/11 0011 下午 14:26
 * @description:
 */
public class MainConstruction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(parameterTool);


        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));

        String topic = "tsy";

        //人力manpower
        String lupolFilter = "lupolFilter";
        String lupolJoinLup = "lupolJoinLup";
        String lupolJoinPayintv = "lupolJoinPayintv";
        String lupolJoinSigndate = "lupolJoinSigndate";
        String lupolJoinRate = "lupolJoinRate";
        String lupolJoinT02salesinfo_k = "lupolJoinT02salesinfo_k";
        //计算constructionPremiums
        String constructionPremiums = "constructionPremiums";

        //中间表
        //lcpol
        /*JobConstruction.midLcpol(env,topic,kafkaProp,
                "KLMIDAPP:LCPOL_CONTNO",
                new String[]{"contno"},
                new String[]{"polno"},
                "");*/

        //lbpol
        /*JobConstruction.midLbpol(env,topic,kafkaProp,
                "KLMIDAPP:LBPOL_CONTNO",
                new String[]{"contno"},
                new String[]{"polno"},
                "");*/

        //人员表
        /*JobConstruction.midT02salesinfo_K(env,topic,kafkaProp,
                "KLMIDAPP:T02SALESINFO_K_SALES_ID",
                new String[]{"sales_id"},
                new String[]{"sales_id"},
                "t02salesinfo_k");*/

        //机构表：关联到分公司
        /*JobConstruction.midT01branchinfo(env,topic,kafkaProp,
                "KLMIDAPP:T01BRANCHINFO_BRANCH_ID",
                new String[]{"branch_id"},
                new String[]{"branch_id"},
                "t01branchinfo");*/
//
        //保全表：保单状态不为当日撤单和犹豫期退保
        /*JobConstruction.midLpedoritem(env,topic,kafkaProp,
                "KLMIDAPP:LPEDORITEM_EDORNO_CONTNO",
                new String[]{"edorno","contno"},
                new String[]{"contno","edoracceptno","edorno","edortype","insuredno","polno"},
                "lpedoritem");*/

        //期趸缴表
        /*JobConstruction.midProduct_config(env,topic,kafkaProp,
                "KLMIDAPP:PRODUCT_CONFIG",
                new String[]{"product_code"},
                new String[]{"product_code"},
                "product_config");*/


        //产品率值配置表
        /*JobConstruction.midProduct_rate_config(env,topic,kafkaProp,
                "KLMIDAPP:PRODUCT_RATE_CONFIG",
                new String[]{"product_code","pay_period"},
                new String[]{"key_id"},
                "Product_rate_config");*/


        /**入职人力关联层*/
        //入职各种人力关联
        JobConstruction.getBranchOffice(env,topic,kafkaProp,constructionPremiums);

        /**人力关联层*/
        //关联LPEDORITEM，保单状态为当日撤单或者犹豫期退保和状态为0
//        JobConstruction.lcpolFilter(env,topic,kafkaProp,lupolFilter);
//        JobConstruction.lbpolFilter(env,topic,kafkaProp,lupolFilter);

        //关联lccont,lbcont，获取签单日期
//        JobConstruction.lupolcFilterSigndate(env,lupolFilter,kafkaProp,lupolJoinSigndate);
//        JobConstruction.lupolbFilterSigndate(env,lupolFilter,kafkaProp,lupolJoinSigndate);

        //关联Lcpol，Lbpol，取主险计划代码，主险缴费年期
//        JobConstruction.lcpolFilterLccont(env,lupolJoinSigndate,kafkaProp,lupolJoinLup);
//        JobConstruction.lbpolFilterLbcont(env,lupolJoinSigndate,kafkaProp,lupolJoinLup);

        //所取保费为期交
//        JobConstruction.lupolFilter(env,lupolJoinLup,kafkaProp,lupolJoinPayintv);

        //关联产品率值表，取率值
//        JobConstruction.lupolFilterRate(env,lupolJoinPayintv,kafkaProp,lupolJoinRate);
        //关联T02
//        JobConstruction.lupol2T02salesinfo_k(env,lupolJoinRate,kafkaProp,lupolJoinT02salesinfo_k);
        //关联T01获取分公司代码
//        JobConstruction.Lup2T01branchinfo(env,lupolJoinT02salesinfo_k,kafkaProp,constructionPremiums);
        /**计算层*/
//        ManpowerJob.construction(env,LupJoinT01branchinfo,kafkaProp);
        ManpowerConstruction.construction(env,constructionPremiums,kafkaProp);

        env.execute("");
    }
}
