package com.pactera;

import com.pactera.yhl.Job;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        System.out.println(configPath);
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(params);

        String hiveTableName = "kl_core.ldcode";
        String clickhouseTableName = "kl_core.ldcode";

        Job.Table1Hive2Clickhouse(env,hiveTableName,clickhouseTableName);
        env.execute(Main.class.getSimpleName());

    }
}
