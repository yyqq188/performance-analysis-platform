package com.pactera.yhl.hivekafka;

import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class FlinkHiveJdbc {
    @SneakyThrows
    public static void main(String[] args) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://10.5.2.134:2181/tmp","hive","hive");
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery("select * from tmp.o_lis_anychatcont limit 10");
        while (rs.next()){
            System.out.println(rs.getString(1) + "," + rs.getString(2));
        }
        rs.close();
        st.close();
        con.close();
    }
}
