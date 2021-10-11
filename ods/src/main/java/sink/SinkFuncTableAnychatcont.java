package sink;

import entity.KLEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yhl
 * @time 2021/9/28 10:12
 * @Desc
 */
public class SinkFuncTableAnychatcont<OUT> extends RichSinkFunction<OUT> implements CheckpointedFunction {
    private static final Logger logger = LoggerFactory.getLogger(SinkFuncTableAnychatcont.class);
    private LinkedBlockingQueue<KLEntity> linkedBlockingQueue;
    private CyclicBarrier cyclicBarrier;
    private int default_client_thread_num;
    private int default_queue_num;

    @Override
    public void open(Configuration parameters) throws Exception {
        Map<String, String> params = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        default_client_thread_num = Integer.parseInt(params.get("default_client_thread_num"));
        default_queue_num = Integer.parseInt(params.get("default_queue_num"));
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(default_client_thread_num,default_client_thread_num,
                0L, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<>());
        linkedBlockingQueue = new LinkedBlockingQueue<>(default_queue_num);
        cyclicBarrier = new CyclicBarrier(default_client_thread_num + 1);
        Connection connection = KLHbaseConnection.getHbaseHbaseConnection(params);
        MultiThreadConsumerClient_V2 multiThreadConsumerClient = new MultiThreadConsumerClient_V2(linkedBlockingQueue,cyclicBarrier,connection);
        for(int i = 0;i < default_client_thread_num;i++){
            threadPoolExecutor.execute(multiThreadConsumerClient);
        }
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {
        linkedBlockingQueue.put((KLEntity) value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        logger.info("start snapshot ...");
        cyclicBarrier.await();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
