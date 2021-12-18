package com.pactera.yhl.apps.develop.kafka2clickhouse;

import com.pactera.yhl.apps.develop.premiums.job.JobPremiums;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MainKafka2CK {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("config_path");
        ParameterTool params = ParameterTool.fromPropertiesFile(configPath);

        env.getConfig().setGlobalJobParameters(parameterTool);

        parameterTool.getProperties();
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", params.toMap().get("kafka_bootstrap_servers"));


        JobKafka2CK.applicationGeneralResult(env,
                "APPLICATION_GENERAL_RESULT_RT",kafkaProp,
                "APPLICATION_GENERAL_RESULT_RT"
                );

        JobKafka2CK.applicationProductResult(env,
                "APPLICATION_PRODUCT_RESULT_RT",
                kafkaProp,
                "APPLICATION_PRODUCT_RESULT_RT");

        JobKafka2CK.applicationProductDetial(env,
                "APPLICATION_PRODUCT_DETIAL_RT",
                kafkaProp,
                "APPLICATION_PRODUCT_DETIAL_RT");

        env.execute(MainKafka2CK.class.getSimpleName());
    }

}
