package com.pactera.yhl.hivekafka;

import com.pactera.yhl.hivekafka.entites.Anychatcont;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class FlinkHiveP {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","hive");
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment .create(settings);

//        ParameterTool tool = ParameterTool.fromArgs(args);
//        String config_path = tool.get("config_path");
//        System.out.println(config_path);

        String name            = "myhive";
        String defaultDatabase = "testyhl";//tableName;
        String hiveConfDir     = "D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\dws\\src\\main\\resources";//config_path;


        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String createDbSql = "select * from student2";

        String[] strings = tableEnv.listTables();
        for (int i = 0; i < strings.length; i++) {
            System.out.println(strings[i]);
        }
        System.out.println("-----------------");
//        Table table = tableEnv.sqlQuery(createDbSql);
//        table.execute();
        CloseableIterator<Row> collect = tableEnv.executeSql(createDbSql).collect();
        while(collect.hasNext()){
            Row next = collect.next();
            System.out.println(next.toString());
        }


    }
}
