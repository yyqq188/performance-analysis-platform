package com.pactera.yhl.apps.warning.sink;

//import com.pactera.yhl.apps.develop.warning.config.MyConfig;
import com.pactera.yhl.apps.warning.config.MyConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AbstractCKSinkSQ<T> extends RichSinkFunction<T> {
    private static Connection connection;
    protected static PreparedStatement statement1;
    protected static PreparedStatement statement2;
    private static ResultSet results;
    protected static String sql1;
    protected static String sql2;
    protected static String tableName;

    @Override
    public void open(Configuration parameters) throws Exception {

        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        ClickHouseDataSource dataSource = new ClickHouseDataSource(MyConfig.CKURL);
        connection = dataSource.getConnection(MyConfig.CKUSERNAME, MyConfig.CKPASSWORD);


//        Properties properties = new Properties();
//        properties.setProperty("apache_buffer_size","655350"); //65535
//        properties.setProperty("buffer_size","655350"); //65535
//        properties.setProperty("MAX_TOTAL","20000");  //10000
//        connection = DriverManager.getConnection(address,properties);
        //用的默认配置
//        connection = DriverManager.getConnection(address);
//        connection = DriverManager.getConnection(address,user,password);

//        statement = connection.createStatement();
    }

    @Override
    public void close() throws Exception {

        if (results != null) {
            results.close();
        }
        if (statement1 != null) {
            statement1.close();
        }
        if (statement2 != null) {
            statement2.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        System.out.println(value);

        Field[] declaredFields = value.getClass().getDeclaredFields();
        List<String> arrs = new ArrayList<>();
        for (Field f : declaredFields) {
            Object o = f.get(value);
            System.out.println(o);
            try {
                arrs.add(o.toString());
            } catch (Exception e) {
                arrs.add("null");
            }

        }
        sql1 = String.format("insert into " + tableName + " values (\'%s\')",
//                String.join("\',\'", arrs));
        sql2 = String.format("OPTIMIZE TABLE " + tableName + " final"));
//        System.out.println(sql1);
        statement1 = connection.prepareStatement(sql1);
        statement1.executeUpdate(sql1);
        statement2 = connection.prepareStatement(sql2);
        statement2.execute(sql2);

//        results = statement.executeQuery(sql);
//        ResultSetMetaData rsmd = results.getMetaData();
    }
}
