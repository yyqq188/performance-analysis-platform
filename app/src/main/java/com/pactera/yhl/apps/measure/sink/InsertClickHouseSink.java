package com.pactera.yhl.apps.measure.sink;

import com.pactera.yhl.apps.measure.entity.AssessResult;
import com.pactera.yhl.apps.measure.entity.FactWageBase;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class InsertClickHouseSink extends RichSinkFunction<FactWageBase> {
    public static String username = "default";
    public static String password = "default";
    public static String address = "jdbc:clickhouse://10.5.2.134:8123";
    public static Connection connection = null;
    public static Statement stmt = null;
    public static int i = 1;
    //落地表名
    public static String tableName = "kl_base.APPLICATION_ASSESSMENT_PERSION_RESULT_RT";

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        connection = DriverManager.getConnection(address,username,password);
        stmt = connection.createStatement();
    }

    @Override
    public void close() throws Exception {
        if(connection != null){
            connection.close();
        }
        if(stmt != null){
            stmt.close();
        }
    }

    @Override
    public void invoke(FactWageBase value, Context context) throws Exception {
        AssessResult assessResult = new AssessResult();
        String keys = "";
        String values = "";
        Field[] fields = assessResult.getClass().getDeclaredFields();
        int count = 0;
        int length = fields.length;
        for (Field field : fields) {
            String fieldName = field.getName();
            String fieldValue = (String)getGetMethod(value, fieldName);
            if(count < length){
                keys = keys + fieldName + ",";
                values = values + "'"+ fieldValue + "'" + ",";
            }else {
                keys = keys + fieldName;
                values = values + "'"+ fieldValue + "'";
            }
            count++;
        }
        String sql = "INSERT INTO kl_base.APPLICATION_ASSESSMENT_PERSION_RESULT_RT (" + keys + ")\n" +
                "VALUES(" + values + ");";
    }

    public static Object getGetMethod(Object ob , String name)throws Exception{
        Method[] m = ob.getClass().getMethods();
        for(int i = 0;i < m.length;i++){
            if(("get"+name).toLowerCase().equals(m[i].getName().toLowerCase())){
                return m[i].invoke(ob);
            }
        }
        return null;
    }
}
