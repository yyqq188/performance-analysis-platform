package com.pactera.yhl.hivekafka2;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class DirectHive {
    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream("D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\dws\\src\\main\\resources\\conf.properties"));
            prop.load(in);
//            Float.valueOf("");
        }catch (Exception e){
            e.printStackTrace();
        }

        Class.forName(prop.getProperty("driver"));
        String querySQL = prop.getProperty("sql");
        Connection con = DriverManager.getConnection(prop.getProperty("url"));
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery(querySQL);
        while(res.next()){
//            System.out.println("Result: key:"+res.getString(1) +"  –>  value:" +res.getString(2));
            System.out.println(res.getString(1));
        }
    }
}
