package com.pactera.yhl.hive2hbase;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Hive2HbaseMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        System.out.println(configPath);
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(params);

        String hiveTableName = "kl_core.ldcode";
        String hbaseTableName = "testyhl:ldcode";
        String[] rowkeys = {"code"};

        Job.Table1Hive2Hbase(env,hiveTableName,hbaseTableName,rowkeys);
        env.execute("");
    }
}
