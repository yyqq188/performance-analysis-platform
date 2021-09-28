import entity.KLEntity;
import entity.Anychatcont;
import job.jobDefine;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author YHl
 * @time 2021/9/27 17:21
 * @Desc
 */
public class MainV2 {
    private static final Logger logger = LoggerFactory.getLogger(MainV2.class.getName());
    public static void main(String[] args) throws Exception {
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        String config_path = paraTool.get("config_path");
        ParameterTool conf = ParameterTool.fromPropertiesFile(config_path);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(conf);
        env.setParallelism(Integer.parseInt(conf.get("parallelism")));
//        env.enableCheckpointing(Integer.parseInt(conf.get("checkpoint_interval")), CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend(conf.get("fs_statebackend")));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.parseInt(conf.get("restart_attempts")), Long.parseLong(conf.get("restart_delay_between_attempts"))));
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        checkpointConfig.setTolerableCheckpointFailureNumber(0);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", conf.get("kafka_bootstrap_servers"));
        properties.setProperty("group.id", conf.get("kafka_group_id"));
        properties.setProperty("enable.auto.commit", conf.get("kafka_enable_auto_commit"));
        properties.setProperty("auto.commit.interval.ms", conf.get("kafka_auto_commit_interval"));//自动提交的时间间隔
//        properties.setProperty("auto.offset.reset", "latest"); // latest/earliest

        String topic = conf.get("kafka_topic");
        logger.info("topic is {}",topic);

        jobDefine.jobTableAnychatcont(env,properties,topic);
        KLEntity a = new Anychatcont();

    }
}
