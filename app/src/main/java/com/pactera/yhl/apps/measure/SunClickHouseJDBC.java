package com.pactera.yhl.apps.measure;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.entity.FactWageBase;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SunClickHouseJDBC {
    public static String username = "default";
    public static String password = "default";
    public static String address = "jdbc:clickhouse://10.5.2.134:8123";
    public static ClickHouseConnection conn = null;
    public static Statement stmt = null;
    public static Statement Connection() throws Exception {
        int socketTimeout = 600000;

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(username);
        properties.setPassword(password);
        properties.setSocketTimeout(socketTimeout);
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(address, properties);

        conn = clickHouseDataSource.getConnection();
        try{
            stmt = conn.createStatement();
            return stmt;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static void close() throws SQLException {
        conn.close();
    }

    public static void main(String[] args) throws Exception {
        SunClickHouseJDBC.Connection();
        String value = "{\"agent_grade\":\"A17\",\"agent_id\":\"920108000032\",\"agent_name\":\"\\xE6\\x96\\xB9\\xE5\\x9C\\x86\",\"agentstate\":\"1\",\"assessment_difference\":\"1499175.0\",\"assessment_result\":\"3\",\"assessment_standard\":\"5760000.0\",\"assessmonth\":\"4\",\"d_fyp\":\"0.0\",\"day_id\":\"2021-09-30\",\"department_activity_m\":\"\",\"department_activity_q\":\"\",\"department_activity_rate_m\":\"\",\"department_activity_rate_q\":\"\",\"department_agentcode\":\"\",\"department_assessment\":\"0\",\"department_assessment_m\":\"\",\"department_code\":\"\",\"department_d_fyp\":\"\",\"department_leader\":\"\",\"department_m_fyp\":\"\",\"department_name\":\"\",\"department_q_fyp\":\"\",\"department_rolling_rate\":\"1.00\",\"distinct_activity_m\":\"0\",\"distinct_activity_q\":\"0\",\"distinct_activity_rate_m\":\"0.00\",\"distinct_activity_rate_q\":\"0.0\",\"distinct_assessment\":\"13\",\"distinct_assessment_m\":\"0\",\"distinct_d_fyp\":\"90425.0\",\"distinct_m_fyp\":\"658425.0\",\"distinct_q_fyp\":\"4260825.0\",\"distinct_rolling_rate\":\"1\",\"district_agentcode\":\"920108000032\",\"district_code\":\"920108133000\",\"district_leader\":\"\\xE6\\x96\\xB9\\xE5\\x9C\\x86\",\"district_name\":\"\\xE6\\x96\\xB9\\xE5\\x9C\\x86\\xE6\\x80\\xBB\\xE7\\x9B\\x91\\xE5\\x8C\\xBA\",\"hire_date\":\"2021-06-08\",\"hire_month\":\"2021-06\",\"key_id\":\"2021-09-30#920108000032\",\"" +
                "\":\"2021-09-30 11:57:35\",\"m_fyp\":\"0.0\",\"member_rolling_rate\":\"1\",\"provincecom_code\":\"8633\",\"provincecom_name\":\"\\xE6\\xB5\\x99\\xE6\\xB1\\x9F\\xE5\\x88\\x86\\xE5\\x85\\xAC\\xE5\\x8F\\xB8\",\"q_fyp\":\"0.0\",\"to_agent_grade\":\"A15\",\"workarea\":\"\\xE5\\xAE\\x81\\xE6\\xB3\\xA2\"}";
        FactWageBase factWageBase = JSON.parseObject(value,FactWageBase.class);
        Field[] declaredFields = factWageBase.getClass().getDeclaredFields();
        List<String> arrs = new ArrayList<>();
        for(Field f:declaredFields){
            String o = (String)f.get(factWageBase);
            try{
                arrs.add(o);
            }catch (Exception e){
                arrs.add("null");
            }
        }
        String sql = String.format("insert into " + "kl_base.SUNTEST" + " values (\'%s\')", String.join("\',\'",arrs));
        stmt.execute(sql);

        SunClickHouseJDBC.close();
    }

}
