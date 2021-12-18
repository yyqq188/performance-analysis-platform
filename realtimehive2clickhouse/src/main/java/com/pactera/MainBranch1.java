package com.pactera;

import com.pactera.yhl.Job;
import com.pactera.yhl.demo.ReadHive;
import com.pactera.yhl.demo.WriteClickhouse;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.LinkedBlockingQueue;

public class MainBranch1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        System.out.println(configPath);
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);
        env.getConfig().setGlobalJobParameters(params);

        //application_general_result
        String[] tables = parameterTool.get("tables").split(",");
        for(String table:tables){
            String hiveTableName = "kl_base."+ table;
//            String clickhouseTableName = "kl_base."+table.toUpperCase();
            String clickhouseTableName = "default."+table.toUpperCase();
            Job.Table1Hive2Clickhouse(env,hiveTableName,clickhouseTableName);


        }
//        String hiveTableName = "kl_base.application_general_result";
//        String clickhouseTableName = "kl_base.APPLICATION_GENERAL_RESULT";
//        Job.Table1Hive2Clickhouse(env,hiveTableName,clickhouseTableName);
//        env.execute(MainBranch1.class.getSimpleName());
        env.execute(MainBranch1.class.getSimpleName());
    }

}
