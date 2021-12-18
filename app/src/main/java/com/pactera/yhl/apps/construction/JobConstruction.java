package com.pactera.yhl.apps.construction;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.apps.construction.entity.LupToT02;
import com.pactera.yhl.apps.construction.entity.Product_config;
import com.pactera.yhl.apps.construction.entity.Product_rate_config;
import com.pactera.yhl.apps.construction.join.*;
import com.pactera.yhl.apps.construction.mid.InsertHbase;
import com.pactera.yhl.entity.source.*;
import com.pactera.yhl.transform.TestMapTransformFunc;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author: TSY
 * @create: 2021/11/11 0011 下午 14:34
 * @description:  中间层，关联层
 */
public class JobConstruction {

    /**配置表*/
    public static void midProduct_config(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobConstruction_midProduct_config");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        //kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Product_config)
                .map(x -> (Product_config) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midProduct_rate_config(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobConstruction_midProduct_rate_config");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        //kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Product_rate_config)
                .map(x -> (Product_rate_config) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }


    /**中间层*/
    public static void midLcpol(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobConstruction_midLcpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        //kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }


    public static void midLbpol(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobConstruction_midLbpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        //kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midT02salesinfo_K(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobConstruction_midT02salesinfo_K");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        //kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof T02salesinfok)
                .map(x -> (T02salesinfok) x)
                //.filter(x -> "2021".equals(x.getVersion_id()) && "8".equals(x.getVersion_id()))
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midT01branchinfo(StreamExecutionEnvironment env, String topic, Properties prop,
                                              String tableName, String[] rowkeys,
                                              String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobConstruction_midT01BRANCHINFO");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        //kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midLpedoritem(StreamExecutionEnvironment env, String topic, Properties prop,
                                              String tableName, String[] rowkeys,
                                              String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobConstruction_midLpedoritem");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lpedoritem)
                .map(x -> (Lpedoritem) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }


    /**关联层*/
    //通过关联获取分公司代码
    public static void getBranchOffice(StreamExecutionEnvironment env, String topic,
                                       Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_getBranchOffice");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof T02salesinfok)
                .map(x -> (T02salesinfok) x)
                .addSink(new GetBranchOffice(topicOut));
    }

    public static void lcpolFilter(StreamExecutionEnvironment env, String topic,
                                   Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lcpolFilter");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new lcpolFilter(topicOut));
    }

    public static void lbpolFilter(StreamExecutionEnvironment env, String topic,
                                   Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lbpolFilter");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        //kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new lbpolFilter(topicOut));
    }

    public static void lcpolFilterLccont(StreamExecutionEnvironment env, String topic,
                                   Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lcpolFilterLccont");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupFilter.class))
                .filter(x -> "lcpol".equals(x.getMark()))
                .addSink(new LcpolFilterLccont(topicOut));
    }

    public static void lbpolFilterLbcont(StreamExecutionEnvironment env, String topic,
                                   Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lbpolFilterLbcont");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupFilter.class))
                .filter(x -> "lbpol".equals(x.getMark()))
                .addSink(new LbpolFilterLbcont(topicOut));
    }

    public static void lupol2Lpedoritem(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lupol2Lpedoritem");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        //kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupFilter.class))
                .filter(x -> x != null)
                .addSink(new lupol2Lpedoritem(topicOut));
    }

    public static void lupolFilter(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lupolFilter");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        //kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupFilter.class))
                .filter(x -> x != null)
                .addSink(new lupolFilter(topicOut));
    }

    public static void lupolcFilterSigndate(StreamExecutionEnvironment env, String topic,
                                           Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lupolcFilterSigndate");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupFilter.class))
                .filter(x -> x != null)
                .filter(x -> "lcpol".equals(x.getMark()))
                .addSink(new lupolcFilterSigncom(topicOut));
    }

    public static void lupolbFilterSigndate(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lupolbFilterSigndate");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupFilter.class))
                .filter(x -> x != null)
                .filter(x -> "lbpol".equals(x.getMark()))
                .addSink(new lupolbFilterSigncom(topicOut));
    }

    public static void lupolFilterRate(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lupolFilterRate");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        //kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupFilter.class))
                .filter(x -> x != null)
                .addSink(new lupolFilterRate(topicOut));
    }

    public static void lupol2T02salesinfo_k(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_lupol2T02salesinfo_k");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        //kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupFilter.class))
                .filter(x -> x != null)
                .addSink(new lupol2T02salesinfo_k(topicOut));
    }


    public static void Lup2T01branchinfo(StreamExecutionEnvironment env, String topic,
                                            Properties prop,String topicOut){
        prop.setProperty("group.id","JobConstruction_getBranchOffice");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        //kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(x -> JSONObject.parseObject(x, LupToT02.class))
                .addSink(new Lup2T01(topicOut));
    }
}
