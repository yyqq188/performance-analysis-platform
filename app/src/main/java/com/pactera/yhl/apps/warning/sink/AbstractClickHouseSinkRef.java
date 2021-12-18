package com.pactera.yhl.apps.warning.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author SUN KI
 * @time 2021/11/25 9:37
 * @Desc
 */
public abstract class AbstractClickHouseSinkRef<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    protected static Logger logger = LoggerFactory.getLogger(AbstractClickHouseSinkRef.class);

    private final String query;
    private final int batchInterval;

    private final String username;
    private final String password;
    private final List<String> dbUrls;
    private final String cluster;
    private String dataBase;
    private int port;
    private List<IN> values;

    private Map<String, StatementWeight> allStatementWeightMap;
    private Map<String, StatementWeight> enabledStatementWeightMap;


    public AbstractClickHouseSinkRef(int batchInterval, String username, String password,
                                     String query, List<String> dbUrls, String cluster) {
        this.batchInterval = batchInterval;
        this.username = username;
        this.password = password;
        this.query = query;
        this.dbUrls = dbUrls;
        this.cluster = cluster;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.values = new LinkedList<>();
        this.allStatementWeightMap = new ConcurrentHashMap<>();
        this.enabledStatementWeightMap = new ConcurrentHashMap<>();
        for (String dbURL : dbUrls) {
            ClickHouseDataSource dataSource = new ClickHouseDataSource(dbURL);
            ClickHouseConnection connection;
            if (username == null || username.isEmpty()) {
                connection = dataSource.getConnection();
            } else {
                connection = dataSource.getConnection(username, password);
            }
            dataBase = dataSource.getDatabase();
            port = dataSource.getPort();
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            StatementWeight statementWeight = new StatementWeight();
            statementWeight.preparedStatement = preparedStatement;
            statementWeight.dbUrl = dbURL;
            this.allStatementWeightMap.put(dbURL, statementWeight);
        }
        this.enabledStatementWeightMap = checkAllStatementWeightMap();
        if (this.enabledStatementWeightMap.isEmpty()) {
            throw new RuntimeException("Unable JDBC statement using");
        }
        // 获取集群节点信息
        getStatementWeightFromCluster();
        // 激活定时检测节点是否可用
        scheduleActualization(300, 600, TimeUnit.SECONDS);
    }


    @Override
    public void close() throws Exception {
        flush();
        Collection<StatementWeight> enabledStatementWeightList = this.enabledStatementWeightMap.values();
        for (StatementWeight statementWeight : enabledStatementWeightList) {
            PreparedStatement preparedStatement = statementWeight.preparedStatement;
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
        this.allStatementWeightMap.clear();
        this.enabledStatementWeightMap.clear();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        values.add(value);
        if (values.size() >= batchInterval) {
            // execute batch
            flush();
        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // execute batch
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }


    public abstract void setRecordToStatement(IN value, PreparedStatement preparedStatement) throws SQLException;


    private void scheduleActualization(int initialDelay, int delay, TimeUnit timeUnit) {
        ScheduledConnectionCleaner.INSTANCE.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                    logger.info("检测链接完成,可用链接数：" +  enabledStatementWeightMap.size());
                } catch (Exception e) {
                    logger.error("Unable to actualize urls", e);
                }
            }
        }, initialDelay, delay, timeUnit);
    }

    public synchronized void actualize() {
        try {
            this.enabledStatementWeightMap = checkAllStatementWeightMap();
            getStatementWeightFromCluster();
        } catch (SQLException e) {
            logger.error("updateStatementWeightMap:", e);
        }
    }

    private boolean ping(PreparedStatement preparedStatement) {
        try {
            preparedStatement.execute("SELECT 1");
            return true;
        } catch (Exception e) {
            logger.debug("Unable to connect using:", e);
            return false;
        }
    }

    // casting values as suggested by http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
    protected void setField(Object field, int index, PreparedStatement preparedStatement) throws SQLException {
        try {
            if (field == null) {
                preparedStatement.setNull(index, java.sql.Types.NULL);
                return;
            }
            if (field instanceof String) {
                preparedStatement.setString(index, (String) field);
                return;
            }
            if (field instanceof Byte) {
                preparedStatement.setByte(index, (byte) field);
                return;
            }
            if (field instanceof Integer) {
                preparedStatement.setInt(index, (int) field);
                return;
            }
            if (field instanceof Long) {
                preparedStatement.setLong(index, (long) field);
                return;
            }
            if (field instanceof Float || field instanceof Double) {
                preparedStatement.setDouble(index, (double) field);
                return;
            }
            if (field instanceof java.math.BigDecimal) {
                preparedStatement.setBigDecimal(index, (java.math.BigDecimal) field);
                return;
            }
            if (field instanceof java.sql.Time) {
                preparedStatement.setTime(index, (java.sql.Time) field);
                return;
            }
            if (field instanceof java.sql.Date) {
                preparedStatement.setDate(index, (java.sql.Date) field);
                return;
            }
            if (field instanceof java.sql.Timestamp) {
                preparedStatement.setTimestamp(index, (java.sql.Timestamp) field);
                return;
            }
            preparedStatement.setObject(index, field);
        } catch (ClassCastException e) {
            // enrich the exception with detailed information.
            String errorMessage = String.format(
                    "%s, field index: %s, field value: %s.", e.getMessage(), index, field);
            ClassCastException enrichedException = new ClassCastException(errorMessage);
            enrichedException.setStackTrace(e.getStackTrace());
            throw enrichedException;
        }

    }


    private void flush() throws RuntimeException {
        if (this.values.size() == 0) {
            return;
        }
        int maxRetries = this.enabledStatementWeightMap.size();
        int i = 1;
        while (i <= maxRetries) {
            try {
                PreparedStatement preparedStatement = doSelect().preparedStatement;
                for (IN v : this.values) {
                    setRecordToStatement(v, preparedStatement);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                this.values.clear();
                break;
            } catch (SQLException e) {
                logger.error("ClickHouse executeBatch error, retry times = {}", i, e);
                if (i >= maxRetries) {
                    throw new RuntimeException(e);
                }
                ++i;
            }
        }
    }

    /**
     * 检查 链接是否可用
     *
     * @return
     */
    private Map<String, StatementWeight> checkAllStatementWeightMap() {
        Map<String, StatementWeight> newStatementWeightMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, StatementWeight> entry : this.allStatementWeightMap.entrySet()) {
            if (ping(entry.getValue().preparedStatement)) {
                newStatementWeightMap.put(entry.getKey(), entry.getValue());
            }
        }
        return newStatementWeightMap;
    }

    /**
     * 从集群中获取节点信息
     *
     * @return
     * @throws SQLException
     */
    private void getStatementWeightFromCluster() throws SQLException {
        String dbURL = this.enabledStatementWeightMap.keySet().stream().findAny().orElse(null);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(dbURL);
        ClickHouseConnection connection;
        if (username == null || username.isEmpty()) {
            connection = dataSource.getConnection();
        } else {
            connection = dataSource.getConnection(username, password);
        }
        String sql = "SELECT shard_weight,host_name  FROM  `system`.clusters WHERE cluster = '" + cluster + "'";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery(sql);
        while (resultSet.next()) {
            int weight = resultSet.getInt(1);
            String hostName = resultSet.getString(2);
            //jdbc:clickhouse://aly-hn1-bigdata-cdh-nn-prd-71:8123/ai_order_log_db
            String url = String.format("jdbc:clickhouse://%s:%s/%s", hostName, this.port, this.dataBase);
            StatementWeight statementWeight = this.allStatementWeightMap.get(url);
            if (statementWeight == null) {
                ClickHouseDataSource source = new ClickHouseDataSource(url);
                ClickHouseConnection con;
                if (username == null || username.isEmpty()) {
                    con = source.getConnection();
                } else {
                    con = source.getConnection(username, password);
                }
                PreparedStatement preStatement = con.prepareStatement(query);
                statementWeight = new StatementWeight();
                statementWeight.preparedStatement = preStatement;
                statementWeight.weight = weight;
                statementWeight.dbUrl = url;
                if (ping(preparedStatement) && !this.enabledStatementWeightMap.containsKey(url)) {
                    this.enabledStatementWeightMap.put(url, statementWeight);
                }
                // 添加新节点
                this.allStatementWeightMap.put(url, statementWeight);
            } else {
                // 更新权重
                statementWeight.weight = weight;
            }
        }
    }


    /**
     * 根据权重 获取链接
     *
     * @param
     * @return
     */
    public StatementWeight doSelect() {
        List<StatementWeight> statementWeights = this.enabledStatementWeightMap.values()
                .stream()
                .collect(Collectors.toList());
        // invoker的数量
        int length = statementWeights.size();
        // 每个 invoker 有相同权重
        boolean sameWeight = true;
        // 每个invoker的权重
        int[] weights = new int[length];
        // 第一个 invoker 的权重
        int firstWeight = statementWeights.get(0).weight;
        weights[0] = firstWeight;
        // 权重之和
        int totalWeight = firstWeight;
        for (int i = 1; i < length; i++) {
            int weight = statementWeights.get(i).weight;
            // 保存以待后用
            weights[i] = weight;
            // Sum
            totalWeight += weight;
            if (sameWeight && weight != firstWeight) {
                sameWeight = false;
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // 如果并非每个invoker都具有相同的权重且至少一个invoker的权重大于0，请根据totalWeight随机选择
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            // 根据随机值返回invoker
            for (int i = 0; i < length; i++) {
                offset -= weights[i];
                if (offset < 0) {
                    return statementWeights.get(i);
                }
            }
        }
        // 如果所有invoker都具有相同的权重值或totalWeight = 0，则平均返回。
        return statementWeights.get(ThreadLocalRandom.current().nextInt(length));
    }


    static class StatementWeight {
        String dbUrl;
        int weight = 1;
        PreparedStatement preparedStatement;
    }

    static class ScheduledConnectionCleaner {
        static final ScheduledExecutorService INSTANCE = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());

        static class DaemonThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        }
    }
}
