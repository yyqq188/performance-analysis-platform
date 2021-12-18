package com.pactera.yhl.apps.measure;


import com.pactera.yhl.apps.measure.entity.*;
import com.pactera.yhl.apps.measure.join.Lbopl.*;
import com.pactera.yhl.apps.measure.join.Lcpol.Lccont2Rate;
import com.pactera.yhl.apps.measure.join.Lcpol.Lcopl2Product;
import com.pactera.yhl.apps.measure.join.Lcpol.Lcpol2Lcpol;
import com.pactera.yhl.apps.measure.join.Lcpol.Product2Lccont;
import com.pactera.yhl.apps.measure.join.Utilities.*;
import com.pactera.yhl.apps.measure.join.Prepare.SalesinfoFlat;
import com.pactera.yhl.apps.measure.map.*;
import com.pactera.yhl.apps.measure.map.Lbopl.*;
import com.pactera.yhl.apps.measure.map.lcpol.LccontMapFunc;
import com.pactera.yhl.apps.measure.map.lcpol.Lcp2LcpMapFunc;
import com.pactera.yhl.apps.measure.map.lcpol.LcpMapFunc;
import com.pactera.yhl.apps.measure.map.lcpol.LcpolMapFunc;
import com.pactera.yhl.apps.measure.mid.InsertHbase;
import com.pactera.yhl.entity.*;
import com.pactera.yhl.entity.source.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Properties;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/11 13:33
 */
public class JobMeasure {
    //中间层
    public static void midAssessment_rate_config(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobMeasure_midAssessment_rate_config");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new AssRateConfigMapFunc())
                .filter(x -> x instanceof AssessmentRateConfig)
                .map(x -> (AssessmentRateConfig) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }
    public static void midLbpol(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobMeasure_midLbpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new Lbp2LpdMapFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }
    public static void midLbcont(StreamExecutionEnvironment env, String topic, Properties prop,
                                 String tableName, String[] rowkeys,
                                 String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobMeasure_midLccont");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        source.map(new LbcontMapFunc())
                .filter(x -> x instanceof Lbcont)
                .map(x -> (Lbcont) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midlpedoritem(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobMeasure_midlpedoritem");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new LpedoritemMapFunc())
                .filter(x -> x instanceof Lpedoritem)
                .map(x -> (Lpedoritem) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }
    public static void midLcpol(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobMeasure_midLcpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new LcpolMapFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midLccont(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobMeasure_midLccont");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new LccontMapFunc())
                .filter(x -> x instanceof Lccont)
                .map(x -> (Lccont) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midT02salesinfo(StreamExecutionEnvironment env, String topic, Properties prop,
                                       String tableName, String[] rowkeys,
                                       String[] columnNames, String columnTableName){
        prop.setProperty("group.id","jobmeasure_midT01salesinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new SalesMapFunc())
                .filter(x -> x instanceof T02salesinfok)
                .map(x -> (T02salesinfok) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midT02salesinfoFlat(StreamExecutionEnvironment env, String topic, Properties prop){
        prop.setProperty("group.id","jobmeasure_midT02salesinfoFlat");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new SalesMapFunc())
                .filter(x -> x instanceof T02salesinfok)
                .map(x -> (T02salesinfok) x)
                .addSink(new SalesinfoFlat());
    }

    public static void midT01teaminfo(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName, String[] rowkeys,
                                String[] columnNames, String columnTableName){
        prop.setProperty("group.id","jobmeasure_midT01teaminfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new TeamMapFunc())
                .filter(x -> x instanceof T01teaminfo)
                .map(x -> (T01teaminfo) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midT01branchinfo(StreamExecutionEnvironment env, String topic, Properties prop,
                                        String tableName, String[] rowkeys,
                                        String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobMeasure_midT01branchinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new BranchMapFunc())
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midFactWageBase(StreamExecutionEnvironment env, String topic, Properties prop,
                                        String tableName, String[] rowkeys,
                                        String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobMeasure_midFactWageBase");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new FactMapFunc())
                .filter(x -> x instanceof FactWageBase)
                .map(x -> (FactWageBase) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));

    }

    //关联层
    //-----------------------------------------------
    //lbpol
    public static void lbpol2lpedoritem(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_lbpol2lpedoritem");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new Lbp2LpdMapFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new Lbpol2Lpedoritem(topicOut));
    }
    public static void lpedoritem2lbpol(StreamExecutionEnvironment env, String topic,
                                        Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_lpedoritem2lbpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new Lbp2LpdMapFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new Lpedoritem2Lbpol(topicOut));
    }
    public static void lbpol2product(StreamExecutionEnvironment env, String topic,
                                        Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_lbpol2product");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new Lbp2LpdMapFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new Lbpol2Product(topicOut));
    }
    public static void product2lbcont(StreamExecutionEnvironment env, String topic,
                                     Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_product2lbcont");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new Lbp2LpdMapFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new Product2Lbcont(topicOut));
    }
    public static void lbcont2rate(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_lbcont2rate");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer)
                .map(new Lbp2LpdMapFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new Lbcont2Rate(topicOut));
    }

    //-----------------------------------------------
    //lcpol
    public static void lcpol2lcpol(StreamExecutionEnvironment env, String topic,
                                     Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_lcpol2lcpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new Lcp2LcpMapFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new Lcpol2Lcpol(topicOut));

    }
    public static void lcpol2product(StreamExecutionEnvironment env, String topic,
                                   Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_lcpol2product");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new LcpMapFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new Lcopl2Product(topicOut));

    }
    public static void product2lccont(StreamExecutionEnvironment env, String topic,
                                     Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_product2lccont");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new LcpMapFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new Product2Lccont(topicOut));

    }
    public static void lccont2rate(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_lccont2rate");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new LcpMapFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new Lccont2Rate(topicOut));
    }

    //-----------------------------------------------
    public static void rate2salesinfo(StreamExecutionEnvironment env, String topic,
                                            Properties prop,String topicOut){
        prop.setProperty("group.id","JobMeasure_rate2salesinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new Rate2SalesMapFunc())
                .filter(x -> x instanceof Rate2Lpol)
                .addSink(new Rate2Salesinfo(topicOut));

    }

    public static void salesinfo2bteaminfo(StreamExecutionEnvironment env, String topic,
                                           Properties prop, String topicOut) throws IOException {
        prop.setProperty("group.id","JobMeasure_salesinfo2branchinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new Sales2BranchMapFunc())
                .filter(x -> x instanceof Lpol2salesinfo)
                .map(x -> (Lpol2salesinfo) x)
                .addSink(new Salesinfo2Teaminfo(topicOut));

    }

    public static void teaminfo2salesinfo(StreamExecutionEnvironment env, String topic,
                                           Properties prop, String topicOut){
        prop.setProperty("group.id","JobMeasure_teaminfo2salesinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new Sales2TeamMapFunc())
                .filter(x -> x instanceof Sales2Team)
                .map(x -> (Sales2Team) x)
                .addSink(new Teaminfo2Salesinfo(topicOut));

    }

    public static void salesinfo2branchinfo(StreamExecutionEnvironment env, String topic,
                                          Properties prop, String topicOut) throws IOException {
        prop.setProperty("group.id","JobMeasure_salesinfo2branchinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new Sales2Branch())
                .filter(x -> x instanceof Measure1)
                .map(x -> (Measure1) x)
                .addSink(new Salesinfo2Branchinfo(topicOut));

    }

    public static void branchinfo2aggregate(StreamExecutionEnvironment env, String topic,
                                            Properties prop, String topicOut){
        prop.setProperty("group.id","JobMeasure_branchinfo2aggregate");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        source.map(new Branch2AggMapFunc())
                .filter(x -> x instanceof Measure1)
                .map(x -> (Measure1) x)
                .addSink(new Branchinfo2Aggregate(topicOut));
    }

}
