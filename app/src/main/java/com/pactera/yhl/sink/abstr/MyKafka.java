package com.pactera.yhl.sink.abstr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyKafka {
    public static Configuration createConfiguration(Properties params){
        String zkUrl = params.getProperty("zkquorum");
        String zkPort = params.getProperty("zkport");


        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, zkUrl);
        hbaseConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        hbaseConfig.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "30000");
        hbaseConfig.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "30000");
        //认证
        hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");

        return hbaseConfig;
    }

    public static Properties getProperties(String configPath) throws IOException{
        System.out.println("---+++"+configPath);
        InputStream inputStream = new FileInputStream(new File(configPath));
        Properties properties = new Properties();
        properties.load(inputStream);
        inputStream.close();
        return properties;

    }
}
