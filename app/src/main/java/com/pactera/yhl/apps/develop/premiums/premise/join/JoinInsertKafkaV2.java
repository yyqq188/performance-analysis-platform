package com.pactera.yhl.apps.develop.premiums.premise.join;

import com.alibaba.fastjson.JSON;
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
import java.text.SimpleDateFormat;
import java.util.*;

public class JoinInsertKafkaV2<OUT> extends RichSinkFunction<OUT> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName;
    private Connection connection;
    protected KafkaProducer<String,String> producer;
    protected String topic ;

    protected Map<String,String> joinFieldsDriver;
    protected Set<String> otherFieldsDriver;
    protected Set<String> fieldsHbase;
    protected Class<?> hbaseClazz;
    protected Class<?> kafkaClazz;
    protected Map<String,String> filterMapDriver;
    protected Map<String,String> filterMapHbase;



    public JoinInsertKafkaV2(String tableName, String topic,
                             Map<String,String> joinFieldsDriver, Set<String> otherFieldsDriver,
                             Set<String> fieldsHbase, Class<?> hbaseClazz,
                             Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                             Map<String,String> filterMapHbase){
        this.tableName = tableName;//HBase中间表名
        this.topic = topic; //"testyhlv3";  //目的topic

        this.joinFieldsDriver = joinFieldsDriver;
        this.otherFieldsDriver = otherFieldsDriver;
        this.fieldsHbase = fieldsHbase;
        this.hbaseClazz = hbaseClazz;
        this.kafkaClazz = kafkaClazz;
        this.filterMapDriver = filterMapDriver;
        this.filterMapHbase = filterMapHbase;

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
        String rowkeystr = "";
        //这是对product_code进行处理

        Map<String,Object> productRateConfigRowkey = new HashMap<>();

        for(Field f:value.getClass().getDeclaredFields()){
            if(joinFieldsDriver.keySet().contains(f.getName())){
                //这里对关联的字段进行特殊处理
                if(f.getName().equals("managecom")){
                    joinFieldsDriver.put(f.getName(),Util.toSubString(f.get(value)));
                }else if(f.getName().equals("branch_id_full")){
                    joinFieldsDriver.put(f.getName(),f.get(value).toString().split("-")[1]);
                }
                else if(f.getName().equals("contplancode")){
                    productRateConfigRowkey.put("contplancode",f.get(value));
                    joinFieldsDriver.put(f.getName(),"");
                }else if(f.getName().equals("riskcode")){
                    productRateConfigRowkey.put("riskcode",f.get(value));
                    joinFieldsDriver.put(f.getName(),"");
                }else if(f.getName().equals("payyears")){
                    productRateConfigRowkey.put("payyears",f.get(value));
                    joinFieldsDriver.put(f.getName(),"");
                }
                else{
                    joinFieldsDriver.put(f.getName(),Util.toString(f.get(value)));
                }
            }

            if(otherFieldsDriver.contains(f.getName())){
                if(filterMapDriver.keySet().contains(f.getName())
                        && filterMapDriver.get(f.getName()+"").equals(f.get(value)+"")){
                    String methodName = "set"+Util.LargerFirstChar(f.getName());
                    Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                    method.invoke(kafkaClazzObj,Util.toString(f.get(value)));
                }else{
                    String methodName = "set"+Util.LargerFirstChar(f.getName());
                    Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                    method.invoke(kafkaClazzObj,Util.toString(f.get(value)));
                }
            }
        }

        String productRateConfigRowkeyStr = "";
        if(productRateConfigRowkey.size() != 0){
            if(productRateConfigRowkey.get("contplancode") != null){
                productRateConfigRowkeyStr += productRateConfigRowkey.get("contplancode");
            }
            if("".equals(productRateConfigRowkeyStr)|| productRateConfigRowkeyStr.length() == 0){
                productRateConfigRowkeyStr += productRateConfigRowkey.get("riskcode");
            }
            productRateConfigRowkeyStr += productRateConfigRowkey.get("payyears");
            joinFieldsDriver.put("productRateConfigRowkeyStr",productRateConfigRowkeyStr);
        }


        System.out.println(joinFieldsDriver);
        for(String fstr: joinFieldsDriver.values()){

            rowkeystr += fstr;
        }

        System.out.println(hbaseClazz +" ==+++== "+rowkeystr);
        result = Util.getHbaseResultSync(rowkeystr,hTable);

        if(result.value() == null) {
            producer.send(new ProducerRecord<>(topic,
                    JSON.toJSONString(kafkaClazzObj)));

        }else{
            for(Cell cell:result.listCells()){
                String valueJson = Bytes.toString(CellUtil.cloneValue(cell));
                Object o = JSON.parseObject(valueJson, hbaseClazz);

                for(String fieldName:fieldsHbase){
                    //获得hbase的值
                    Field field = hbaseClazz.cast(o).getClass().getField(fieldName);
                    String v = Util.toString(field.get(hbaseClazz.cast(o)));
                    //将hbase的值赋值到kafka实体类中
                    String methodName = "set"+Util.LargerFirstChar(fieldName);
                    Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                    method.invoke(kafkaClazzObj,v);

                }


                int size = filterMapHbase.size();
                int flagTotalNum = 0;


                for(String keyField: filterMapHbase.keySet()){
                    if (filterMapHbase.get(keyField).equals("function_date")){
                        Field field = kafkaClazzObj.getClass().getField(keyField);
                        String v = Util.toString(field.get(kafkaClazzObj)).split("\\s+")[0];
                        System.out.println(v);
                        String todayStr = new SimpleDateFormat("yyyy-MM-dd")
                                .format(new Date(System.currentTimeMillis()));
                        if(todayStr.equals(v)){
                            //将hbase的值赋值到kafka实体类中
                            String methodName = "set"+Util.LargerFirstChar(keyField);
                            Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                            method.invoke(kafkaClazzObj,todayStr);

                            flagTotalNum += 1;
                        }
                    }
                    else  if(filterMapHbase.get(keyField).equals("function_substr")){
                        Field field = kafkaClazzObj.getClass().getField(keyField);
                        String v = Util.toString(field.get(kafkaClazzObj));
                        String vv = v.substring(2,v.length());
                        String methodName = "set"+Util.LargerFirstChar(keyField);
                        Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                        method.invoke(kafkaClazzObj,vv);

                        flagTotalNum += 1;
                    }
                    else {
                        int flagNum = 0;
                        String[] key_values = filterMapHbase.get("key_value").split(",");
                        int len = key_values.length;
                        for(String key_value:key_values){
                            System.out.println(key_value);
                            String fieldName = key_value.split("=")[0];
                            String flagValue = key_value.split("=")[1];
                            Field field = kafkaClazzObj.getClass().getField(fieldName);
                            String v = Util.toString(field.get(kafkaClazzObj));
                            if(flagValue.equals(v)){
                                String methodName = "set"+Util.LargerFirstChar(fieldName);
                                Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
                                method.invoke(kafkaClazzObj,v);
                                flagNum+=1;
                            }
                        }
                        System.out.println("flagNum "+flagNum);
                        System.out.println("len "+len);
                        if(flagNum == len){
                            flagTotalNum += 1;
                        }

                    }
                }
                System.out.println("size "+size);
                System.out.println("flagTotalNum "+flagTotalNum);
                if(size == flagTotalNum){
                    producer.send(new ProducerRecord<>(topic,
                            JSON.toJSONString(kafkaClazzObj)));
                }


            }
        }
    }
}
