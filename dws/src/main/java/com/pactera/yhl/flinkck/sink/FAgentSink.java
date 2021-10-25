package com.pactera.yhl.flinkck.sink;

import com.pactera.yhl.flinkck.config.Config;
import com.pactera.yhl.flinkck.entity.FAgent;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FAgentSink {


    public static void FAgentSink(SingleOutputStreamOperator<FAgent> source){
        String sql = Config.FAGENT_SQL;
        JdbcStatementBuilder<FAgent> fAgent = new JdbcStatementBuilder<FAgent>() {
            @SneakyThrows
            @Override
            public void accept(PreparedStatement preparedStatement, FAgent fAgent) throws SQLException {
                Class<? extends FAgent> aClass = fAgent.getClass();
                int num = 1;
                for(Field field:aClass.getDeclaredFields()){
                    field.setAccessible(true);
                    preparedStatement.setString(num,field.get(fAgent).toString());
                    num +=1;
                }


            }
        };
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder().withBatchSize(Config.BATCH_NUM).withBatchIntervalMs(Config.INTERVAL_TIME_MS).build();
        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(Config.CLICKHOUSE_DRIVER)
                .withUrl(Config.CLICKHOUSE_URL).build();

        source.addSink(JdbcSink.sink(sql,fAgent,executionOptions,jdbcConnectionOptions));

    }

}
