package com.pactera.yhl.hivekafka;

import com.pactera.yhl.hivekafka.entites.Anychatcont;
import com.pactera.yhl.hivekafka.entites.Student;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.types.Row;

import java.util.List;

public class FlinkHive {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","hive");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment  tableEnv = StreamTableEnvironment .create(env,settings);

//        ParameterTool tool = ParameterTool.fromArgs(args);
//        String config_path = tool.get("config_path");
//        String tableName = tool.get("table");
//        String sql = tool.get("sql");
//        System.out.println(config_path);

        String name            = "myhive";
        String defaultDatabase = "testyhl";//tableName;
        String hiveConfDir     = "D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\dws\\src\\main\\resources";//config_path;

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String createDbSql = "select * from student";//sql;

//        String[] strings = tableEnv.listTables();
//        for (int i = 0; i < strings.length; i++) {
//            System.out.println(strings[i]);
//        }
//
//        Table table = tableEnv.sqlQuery(createDbSql);
//        table.printSchema();


        Table table = tableEnv.sqlQuery(createDbSql);
//        table.execute().print();
        DataStream<Student> studentDataStream = tableEnv.toAppendStream(table, Student.class);
        studentDataStream.print("#######");

        env.execute();
    }
}
