package job;

import map.MapFuncTableAnychatcont;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import sink.SinkFuncTableAnychatcont;

import java.util.Properties;

/**
 * @author yhl
 * @time 2021/9/27 17:16
 * @Desc
 */
public class jobDefine {
    public static void jobTableAnychatcont(StreamExecutionEnvironment env, Properties properties, String topic) throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
//        kafkaConsumer.setStartFromTimestamp(Long.parseLong(ts));
        kafkaConsumer.setStartFromEarliest();
//        kafkaConsumer.setStartFromLatest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont()).print();
//                .filter(x -> x != null)
//                .addSink(new SinkFuncTableAnychatcont())
//                .name("sinkToHBaseTable");
//        env.addSource(kafkaConsumer).print();
        env.execute(jobDefine.class.getName());
    }
}
