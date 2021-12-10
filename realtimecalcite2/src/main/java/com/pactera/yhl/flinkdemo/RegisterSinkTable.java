package com.pactera.yhl.flinkdemo;

import com.pactera.yhl.join.TableInfo;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class RegisterSinkTable {
    public static void registerSinkTable(TableInfo tableInfo, StreamTableEnvironment tbEnv){
        Properties props = new Properties();
        String tableName = tableInfo.getTableName();
        if("console".equals(props.getProperty("type"))){
//            ConsoleTableSink consoleTableSink = new ConsoleTableSink();
        }
    }


}
