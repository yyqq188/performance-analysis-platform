package com.pactera.yhl.flinkck.config;

public class Config {
    public static final String FAGENT_SQL = "insert into fagent values (" +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?)";
    public static final String FPOLICY_SQL = "insert into fpolicy values (" +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?";
    public static final int BATCH_NUM = 100;
    public static final int INTERVAL_TIME_MS = 1000;
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://39.106.163.125:8123/default";
}
