package com.pactera.yhl.apps.develop.premiums.premise.join;

import com.pactera.yhl.apps.develop.premiums.entity.LbpolKafka03;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity02;
import com.pactera.yhl.apps.develop.premiums.job.ComplexLogic;
import com.pactera.yhl.apps.develop.premiums.job.ComplexLogicLB;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.sink.abstr.MyKafka;
import com.pactera.yhl.util.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JoinInsertKafkaSHTLB extends RichSinkFunction<LbpolKafka03> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName;
    private Connection connection;
    protected KafkaProducer<String,String> producer;
    protected String topicOut ;
    protected String topicInt ;

    protected Map<String,String> joinFieldsDriver;
    protected Set<String> otherFieldsDriver;
    protected Set<String> fieldsHbase;
    protected Class<?> hbaseClazz;
    protected Class<?> kafkaClazz;
    protected Map<String,String> filterMapDriver;
    protected Map<String,String> filterMapHbase;



    public JoinInsertKafkaSHTLB(String tableName, String topicInt, String topicOut) throws Exception {
        this.tableName = tableName;//HBase中间表名
        this.topicOut = topicOut;
        this.topicInt = topicInt;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");

//        String config_path = "D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\app\\src\\main\\resources\\configuration.properties";
        connection = MyHbaseCli.hbaseConnection(config_path);

        //kafka的配置
        final Properties props = MyKafka.getProperties(config_path);
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("bootstrap.servers",props.getProperty("kafka_bootstrap_servers"));

        producer=new KafkaProducer<String, String>(kafkaProps);

        //创建目的中间的topic
        String kafkaservers = props.getProperty("kafka_bootstrap_servers");
        boolean isExists = Util.topicExists(kafkaservers,topicOut);
        if(!isExists){
            Util.createTopic(kafkaservers,topicOut);
        }

        boolean isExistsInt = Util.topicExists(kafkaservers,topicInt);
        if(!isExistsInt){
            Util.createTopic(kafkaservers,topicInt);
        }


    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(LbpolKafka03 value, Context context) throws Exception {
        HTable hTable = null;
        try{
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            handle(value,context,hTable);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void handle(LbpolKafka03 lbpolKafka03, Context context, HTable hTable) throws Exception {
        System.out.println("handle ing ....");

        HTable midHbase =(HTable) connection.getTable(TableName.valueOf("KLMIDAPPRUN:productcode_intv_payyears"));
        ComplexLogicLB.complexLogic(lbpolKafka03,topicInt,topicOut,hTable,midHbase,producer);


    }
}
