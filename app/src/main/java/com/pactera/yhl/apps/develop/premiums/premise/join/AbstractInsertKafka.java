package com.pactera.yhl.apps.develop.premiums.premise.join;

import com.pactera.yhl.sink.abstr.MyHbaseCli;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public abstract class AbstractInsertKafka<OUT> extends RichSinkFunction<OUT> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName = null;
    private Connection connection;
    protected KafkaProducer<String,String> producer;
    protected String topic ;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        String configPath = "D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\app\\src\\main\\resources\\configuration.properties";
        connection = MyHbaseCli.hbaseConnection(configPath);

        //kafka的配置
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("bootstrap.servers",params.get("kafka_bootstrap_servers"));

        producer=new KafkaProducer<String, String>(kafkaProps);

    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {
        HTable hTable = null;
        try{
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            handle(value,context,hTable);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public abstract void handle(OUT value, Context context,HTable hTable) throws Exception;
}