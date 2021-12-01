package com.pactera.yhl;

import com.pactera.yhl.map.TestMapTransformFunc;
import com.pactera.yhl.sink.MyClickhouseSink;
import com.pactera.yhl.source.MyHiveSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {
    public static void Table1Hive2Clickhouse(StreamExecutionEnvironment env,
                                             String hiveTableName,String clickhouseTableName){

        env.addSource(new MyHiveSource(hiveTableName))
                .map(new TestMapTransformFunc())
                .addSink(new MyClickhouseSink<>(clickhouseTableName));
    }
}
