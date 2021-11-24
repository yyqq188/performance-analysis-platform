package com.pactera.yhl.apps.develop.premiums.premise.join;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.entity.source.T02salesinfok;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.sink.abstr.MyKafka;
import com.pactera.yhl.util.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JoinInsertKafka<OUT> extends RichSinkFunction<OUT> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName;
    private Connection connection;
    protected KafkaProducer<String,String> producer;
    protected String topic ;

    protected Set<String> joinFieldsDriver;
    protected Set<String> otherFieldsDriver;
    protected Set<String> fieldsHbase;
    protected Class<?> hbaseClazz;
    protected Class<?> kafkaClazz;
    protected Map<String,String> filterMap;



    public JoinInsertKafka(String tableName, String topic,
                          Set<String> joinFieldsDriver, Set<String> otherFieldsDriver,
                          Set<String> fieldsHbase, Class<?> hbaseClazz,
                          Class<?> kafkaClazz, Map<String,String> filterMap){
        this.tableName = tableName;//HBase中间表名
        this.topic = topic; //"testyhlv3";  //目的topic

        this.joinFieldsDriver = joinFieldsDriver;
        this.otherFieldsDriver = otherFieldsDriver;
        this.fieldsHbase = fieldsHbase;
        this.hbaseClazz = hbaseClazz;
        this.kafkaClazz = kafkaClazz;
        this.filterMap = filterMap;

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
        boolean isExists = Util.topicExists(kafkaservers,topic);
        if(!isExists){
            Util.createTopic(kafkaservers,topic);
        }


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
    public void handle(OUT value, Context context, HTable hTable) throws Exception {

        Result result = null;
        Object kafkaClazzObj = kafkaClazz.newInstance();
        for(Field f:value.getClass().getDeclaredFields()){
            if(joinFieldsDriver.contains(f.getName())){
                result = Util.getHbaseResultSync(f.get(value)+"",hTable);
            }
            if(otherFieldsDriver.contains(f.getName())){
                String methodName = "set"+Util.LargerFirstChar(f.getName());
                Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                method.invoke(kafkaClazzObj,f.get(value).toString());

            }
        }

        for(Cell cell:result.listCells()){
            String valueJson = Bytes.toString(CellUtil.cloneValue(cell));
            Object o = JSON.parseObject(valueJson, T02salesinfok.class);

            for(String fieldName:fieldsHbase){
                if(filterMap.size() == 0){
                    //获得hbase的值
                    Field field = T02salesinfok.class.cast(o).getClass().getField(fieldName);
                    String v = field.get(T02salesinfok.class.cast(o)).toString();
                    //将hbase的值赋值到kafka实体类中
                    String methodName = "set"+Util.LargerFirstChar(fieldName);
                    Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                    method.invoke(kafkaClazzObj,v);
                }else{
                    if(filterMap.keySet().contains(fieldName)){
                        Field field = T02salesinfok.class.cast(o).getClass().getField(fieldName);
                        String v = field.get(T02salesinfok.class.cast(o)).toString();
                        if(filterMap.get(fieldName).equals(v)){
                            //将hbase的值赋值到kafka实体类中
                            String methodName = "set"+Util.LargerFirstChar(fieldName);
                            Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                            method.invoke(kafkaClazzObj,v);
                        }
                    }
                }
            }
            //判断是否有过滤，有过滤的话，如何处理
            producer.send(new ProducerRecord<>(topic,
                    JSON.toJSONString(kafkaClazzObj)));
        }
    }


}