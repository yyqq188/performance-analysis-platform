package com.pactera.yhl.join;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) {
        String REG_CREATE = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";
        String createSql="CREATE TABLE orders(" + "    orderId varchar," + "    gdsId varchar,"
                + "    orderTime varchar" + " )WITH(" + "    type = 'kafka',"
                + "    kafka.bootstrap.servers = 'localhost:9092'," + "    kafka.topic = 'topic1',"
                + "    kafka.group.id = 'gId1'," + "    sourcedatatype ='json'" + " );";
        Pattern pattern = Pattern.compile(REG_CREATE);
        Matcher matcher = pattern.matcher(createSql);
        TableInfo tableInfo = new TableInfo();

        if(matcher.find()){
            System.out.println(matcher.group(0));
            String tableName = matcher.group(1);
            String fieldInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);
            Map<String, String> stringStringMap = parseFieldstr(fieldInfoStr);
            Properties properties = parseProps(propsStr);

            tableInfo.setTableName(tableName);
            tableInfo.setFieldInfo(stringStringMap);
            tableInfo.setProps(properties);
            if(Boolean.valueOf(tableInfo.getProps().getProperty("isSideTable","false"))){
                tableInfo.setIsSideTable(true);
            }
        }

    }
    public static Map<String,String> parseFieldstr(String fieldStr){
        Map<String,String> fieldInfo = new HashMap<>();
        String[] arrayFields = fieldStr.split(",");
        for(String field:arrayFields){
            String[] split = field.trim().split("\\s+");
            fieldInfo.put(split[0],split[1]);
        }
        return fieldInfo;
    }
    public static Properties  parseProps(String propsStr){
        Properties properties = new Properties();
        String[] props = propsStr.split(",");
        for(String prop:props){
            String[] kv = prop.trim().split("=");
            properties.setProperty(kv[0],kv[1]);
        }
        return properties;
    }



}
