package com.pactera.yhl.sink2;

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

public class MyHbaseCli {
    public static Configuration createConfiguration(String zkUrl,String zkPort){


        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, zkUrl);
        hbaseConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        hbaseConfig.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "30000");
        hbaseConfig.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "30000");
        //认证
        hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");

        return hbaseConfig;
    }

    public static Connection hbaseConnection(String zkUrl,String zkPort) throws IOException {

        Configuration conf = MyHbaseCli.createConfiguration(zkUrl,zkPort);
        conf.set("hbase.client.ipc.pool.type","Reusable");  //Reusable也是默认的  RoundRobinPool ThreadLocal
        conf.set("hbase.client.ipc.pool.size","1000");  //1
        conf.set("hbase.hconnection.threads.max","1000"); //256
        conf.set("hbase.hconnection.threads.core","1000");// 256
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("klapp");
        Connection connection = ConnectionFactory.createConnection(conf, User.create(userGroupInformation));
        return connection;
    }

}
