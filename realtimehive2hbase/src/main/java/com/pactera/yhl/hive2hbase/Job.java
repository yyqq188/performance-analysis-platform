package com.pactera.yhl.hive2hbase;

import com.pactera.yhl.hive2hbase.sink.InsertHbase;
import com.pactera.yhl.hive2hbase.source.MyHiveSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {
    public static void Table1Hive2Hbase(StreamExecutionEnvironment env,
                                        String hiveTableName,String hbaseTableName,String[] rowkeys){
        env.addSource(new MyHiveSource(hiveTableName))
                .addSink(new InsertHbase(hbaseTableName,rowkeys));
    }
}
