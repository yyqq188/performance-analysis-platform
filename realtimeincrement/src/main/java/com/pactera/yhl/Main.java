package com.pactera.yhl;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(params);

        //获得kafka的配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));
        String topic = "testyhlv2";

        String tableName = "hbase_test_ldcode";
        String[] rowkeys = {"code"};
        String[] columnNames = {"codetype","code","codename","codealias","comcode","othersign",
                "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};
        String columnTableName = "";

        //获得hdfs hive的配置信息

//        Job.insertHiveTable1( env, topic, props);
        Job.insertHbaseTable2(env,topic,props, tableName,  rowkeys, columnNames, columnTableName);

        env.execute(Main.class.getSimpleName());

    }
}
