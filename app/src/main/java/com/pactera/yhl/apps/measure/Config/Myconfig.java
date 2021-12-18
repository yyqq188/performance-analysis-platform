package com.pactera.yhl.apps.measure.Config;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/12/08 17:26
 */
public class Myconfig {
    public static final String ZKQUORUM = "prod-bigdata-pc10:2181,prod-bigdata-pc14:2181,prod-bigdata-pc15:2181";
    public static final String KAFKAURL = "prod-bigdata-pc2:6667,prod-bigdata-pc3:6667,prod-bigdata-pc4:6667,prod-bigdata-pc14:6667,prod-bigdata-pc15:6667";
    public static final String ZKPORT = "2181";
    public static final String GROUPID = "group_app_warning";
    public static final String CKURL = "jdbc:clickhouse://10.114.10.94:8123/default";
    public static final String CKUSERNAME = "kl";
    public static final String CKPASSWORD = "kl@123";
}
