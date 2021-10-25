package com.pactera.yhl.flinkck.sink;

import com.pactera.yhl.flinkck.config.Config;
import com.pactera.yhl.flinkck.entity.FAgent;
import com.pactera.yhl.flinkck.entity.FPolicy;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FPolicySink {
    public static void FPolicySink(SingleOutputStreamOperator<FPolicy> source){
        String sql = Config.FPOLICY_SQL;
        JdbcStatementBuilder<FPolicy> fPolicy = new JdbcStatementBuilder<FPolicy>() {
            @SneakyThrows
            @Override
            public void accept(PreparedStatement preparedStatement, FPolicy fPolicy) throws SQLException {
                Class<? extends FPolicy> aClass = fPolicy.getClass();
                int num = 1;
                for(Field field:aClass.getDeclaredFields()){
                    field.setAccessible(true);
                    preparedStatement.setString(num,field.get(fPolicy).toString());
                    num +=1;
                }


            }
        };
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder().withBatchSize(Config.BATCH_NUM).withBatchIntervalMs(Config.INTERVAL_TIME_MS).build();
        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(Config.CLICKHOUSE_DRIVER)
                .withUrl(Config.CLICKHOUSE_URL).build();

        source.addSink(JdbcSink.sink(sql,fPolicy,executionOptions,jdbcConnectionOptions));
    }
}
