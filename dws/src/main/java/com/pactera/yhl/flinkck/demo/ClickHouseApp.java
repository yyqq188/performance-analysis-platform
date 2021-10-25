package com.pactera.yhl.flinkck.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ClickHouseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("10.114.10.92", 9191);
        source.map(new MapFunction<String, Tuple3<String,String,String>>() {
                            @Override
                            public Tuple3<String, String, String> map(String s) throws Exception {
                                String[] tu = s.split(",");
                                System.out.println(s);
                                return Tuple3.of(tu[0].trim(),tu[1].trim(),tu[2].trim());
                            }
                        })
                .addSink(JdbcSink.sink(
                                "insert into test_sql values (?,?,?)",(pstmt,x)->{
                                    pstmt.setString(1,x.f0);
                                    pstmt.setString(2,x.f1);
                                    pstmt.setString(3,x.f2);
                        },
                        new JdbcExecutionOptions.Builder().withBatchSize(1).withBatchIntervalMs(3000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                .withUrl("jdbc:clickhouse://39.106.163.125:8123/default")
                                .build()

                ));

        env.execute(ClickHouseApp.class.getSimpleName());
    }
}
