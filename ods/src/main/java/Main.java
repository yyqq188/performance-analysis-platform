import config.MyConfig;
import job.jobDefine;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.error.MarkedYAMLException;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author SUN KI
 * @time 2021/9/27 17:21
 * @Desc
 */
public class Main {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(2);
        // 设置模式为exactly-once并且每隔5s启动一个检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        //设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://prod-bigdata-pc2:50070/tmp/kunlun/ckp_ods"));
//        //重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000L));
        //设置流处理的时间特性为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //限制的是最大可容忍的连续失败数
        checkpointConfig.setTolerableCheckpointFailureNumber(0);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", MyConfig.KAFKAURL);
        properties.setProperty("group.id", MyConfig.GROUPID);
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔
        properties.setProperty("auto.offset.reset", "latest"); // latest/earliest

        String topic = "listables";

        jobDefine.jobTableAnychatcont(env,properties,topic);

    }
}
