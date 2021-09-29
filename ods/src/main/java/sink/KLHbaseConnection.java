package sink;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class KLHbaseConnection {
    public static Connection getHbaseHbaseConnection(Map<String, String> params) throws IOException {
        Configuration conf = createConfiguration(params);
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println("get hbase connection...");
        return connection;

    }
    public static Configuration createConfiguration(Map<String, String> params){

        String zkUrl = params.get("zookeeper");
        String zkPort = params.get("zk_port");
        String rpcPool = params.get("hbase_client_ipc_pool_size");
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, zkUrl);
        hbaseConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        hbaseConfig.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "30000");
        hbaseConfig.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "30000");
        hbaseConfig.set("hbase.client.ipc.pool.size",rpcPool);  //1
//        hbaseConfig.set("hbase.client.ipc.pool.type","Reusable");  //Reusable也是默认的  RoundRobinPool ThreadLocal
//        hbaseConfig.set("hbase.client.ipc.pool.size","1000");  //1
//        hbaseConfig.set("hbase.hconnection.threads.max","1000"); //256
//        hbaseConfig.set("hbase.hconnection.threads.core","1000");// 256

        return hbaseConfig;
    }
}
