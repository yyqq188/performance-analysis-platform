package com.pactera.yhl.apps.develop.premiums.job;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.*;
import com.pactera.yhl.apps.develop.premiums.premise.join.JoinInsertKafka;
import com.pactera.yhl.apps.develop.premiums.premise.join.JoinInsertKafkaAndHbase;
import com.pactera.yhl.apps.develop.premiums.premise.join_bak.Lbpol2Saleinfo;
import com.pactera.yhl.apps.develop.premiums.premise.join_bak.Lcpol2Saleinfo;
import com.pactera.yhl.apps.develop.premiums.premise.mid.InsertHbase;
import com.pactera.yhl.apps.develop.premiums.premise.mid.InsertHbaseOrder;
import com.pactera.yhl.entity.source.*;
import com.pactera.yhl.sink.abstr.AbstractCKSink;
import com.pactera.yhl.transform.TestMapTransformFunc;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JobPremiums {
    //中间层
    public static void midLbpol(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName,String[] rowkeys,
                                String[] columnNames,String columnTableName){
        prop.setProperty("group.id","JobPremiums_midLbpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midLcpol(StreamExecutionEnvironment env, String topic, Properties prop,
                                String tableName,String[] rowkeys,
                                String[] columnNames,String columnTableName){
        prop.setProperty("group.id","JobPremiums_midLcpol2");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }


    public static void midSaleinfoK(StreamExecutionEnvironment env, String topic, Properties prop,
                                    String tableName,String[] rowkeys,
                                    String[] columnNames,String columnTableName){
        prop.setProperty("group.id","JobPremiums_midSaleinfoK2");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof T02salesinfok)
                .map(x -> (T02salesinfok) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midLpedoritem(StreamExecutionEnvironment env, String topic, Properties prop,
                                    String tableName,String[] rowkeys,
                                    String[] columnNames,String columnTableName){
        prop.setProperty("group.id","JobPremiums_midLpedoritem");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lpedoritem)
                .map(x -> (Lpedoritem) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midLbpol2(StreamExecutionEnvironment env, String topic, Properties prop,
                                    String tableName,String[] rowkeys,
                                    String[] columnNames,String columnTableName){
        prop.setProperty("group.id","JobPremiums_midLbpol2");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    public static void midT01branchinfo(StreamExecutionEnvironment env, String topic, Properties prop,
                                    String tableName,String[] rowkeys,
                                    String[] columnNames,String columnTableName){
        prop.setProperty("group.id","JobPremiums_midT01branchinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }

    //为了order的中间层
    public static void midLcpolOrder(StreamExecutionEnvironment env, String topic, Properties prop,
                                     String tableName,String[] rowkeys,String[] columnNames,
                                     String[] orderColumns,String order,Class<?> clazz,
                                     String columnTableName){
        prop.setProperty("group.id","JobPremiums_midLcpolOrder");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
//                .filter(x -> x instanceof T01branchinfo)
//                .map(x -> (T01branchinfo) x)
                .addSink(new InsertHbaseOrder<>(tableName,rowkeys,columnNames,
                        clazz,orderColumns,order,
                        columnTableName));
    }


    //具有order的中间层

    public static void midLbpolOrder(StreamExecutionEnvironment env, String topic, Properties prop,
                                     String tableName,String[] rowkeys,String[] columnNames,
                                     String[] orderColumns,String order,Class<?> clazz,
                                     String columnTableName){
        prop.setProperty("group.id","JobPremiums_midLbpolOrder");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
//                .filter(x -> x instanceof T01branchinfo)
//                .map(x -> (T01branchinfo) x)
                .addSink(new InsertHbaseOrder<>(tableName,rowkeys,columnNames,
                        clazz,orderColumns,order,
                        columnTableName));
    }
    public static void midProductRateConfig(StreamExecutionEnvironment env, String topic, Properties prop,
                                            String tableName,String[] rowkeys,
                                            String[] columnNames,String columnTableName){
        prop.setProperty("group.id","JobPremiums_midProductRateConfig");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof ProductRateConfig)
                .map(x -> (ProductRateConfig) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }


    //关联层

    public static void lcpol2saleinfo(StreamExecutionEnvironment env, String topic,
                                      Properties prop, String topicOut,
                                      String tableName, Map<String,String> joinFieldsDriver,
                                      Set<String> otherFieldsDriver,
                                      Set<String> fieldsHbase, Class<?> hbaseClazz,
                                      Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                      Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_lcpol2saleinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }
    public static void saleinfo2lcpol(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut,
                                      String tableName, Map<String,String> joinFieldsDriver,
                                      Set<String> otherFieldsDriver,
                                      Set<String> fieldsHbase, Class<?> hbaseClazz,
                                      Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                      Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_saleinfo2lcpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof T02salesinfok)
                .map(x -> (T02salesinfok) x)
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }


    public static void PremiumsKafkaEntity01ToBranchId(StreamExecutionEnvironment env, String topic,
                                      Properties prop,String topicOut,
                                      String tableName, Map<String,String> joinFieldsDriver,
                                      Set<String> otherFieldsDriver,
                                      Set<String> fieldsHbase, Class<?> hbaseClazz,
                                      Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                                       Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_PremiumsKafkaEntity01ToBranchId");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity01>() {
                    @Override
                    public PremiumsKafkaEntity01 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity01.class);
                    }
                })
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }

    public static void PremiumsKafkaEntity01ToBranchId2(StreamExecutionEnvironment env, String topic,
                                                       Properties prop,String topicOut,
                                                       String tableName, Map<String,String> joinFieldsDriver,
                                                       Set<String> otherFieldsDriver,
                                                       Set<String> fieldsHbase, Class<?> hbaseClazz,
                                                       Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                                       Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_PremiumsKafkaEntity01ToBranchId2");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity02>() {
                    @Override
                    public PremiumsKafkaEntity02 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity02.class);
                    }
                })
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }
    public static void lcpolTolcpol(StreamExecutionEnvironment env, String topic,
                                    Properties prop,String topicOut,
                                    String tableName, Map<String,String> joinFieldsDriver,
                                    Set<String> otherFieldsDriver,
                                    Set<String> fieldsHbase, Class<?> hbaseClazz,
                                    Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                    Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_lcpolTolcpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity02>() {
                    @Override
                    public PremiumsKafkaEntity02 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity02.class);
                    }
                })
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }


    public static void lcpolToProductRateConfig(StreamExecutionEnvironment env, String topic,
                                    Properties prop,String topicOut,
                                    String tableName, Map<String,String> joinFieldsDriver,
                                    Set<String> otherFieldsDriver,
                                    Set<String> fieldsHbase, Class<?> hbaseClazz,
                                    Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                    Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_lcpolToProductRateConfig");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity03>() {
                    @Override
                    public PremiumsKafkaEntity03 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity03.class);
                    }
                })
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }

    public static void lbpol2lpedoritem(StreamExecutionEnvironment env, String topic,
                                       Properties prop,String topicOut,
                                       String tableName, Map<String,String> joinFieldsDriver,
                                       Set<String> otherFieldsDriver,
                                       Set<String> fieldsHbase, Class<?> hbaseClazz,
                                       Class<?> kafkaClazz,
                                        String hbaseTableName,Map<String,String> outputHbaseRowkey,
                                        Map<String,String> filterMapDriver, Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_lbpol2lpedoritem");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .addSink(new JoinInsertKafkaAndHbase(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,hbaseTableName,outputHbaseRowkey,
                        filterMapDriver,filterMapHbase));
    }


    public static void lpedoritem2lbpol(StreamExecutionEnvironment env, String topic,
                                        Properties prop,String topicOut,
                                        String tableName, Map<String,String> joinFieldsDriver,
                                        Set<String> otherFieldsDriver,
                                        Set<String> fieldsHbase, Class<?> hbaseClazz,
                                        Class<?> kafkaClazz,
                                        String hbaseTableName,Map<String,String> outputHbaseRowkey,
                                        Map<String,String> filterMapDriver,
                                        Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_lpedoritem2lbpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lpedoritem)
                .map(x -> (Lpedoritem) x)
                .addSink(new JoinInsertKafkaAndHbase(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,
                        hbaseTableName,outputHbaseRowkey,
                        filterMapDriver,filterMapHbase));
    }


    public static void lbpolKafka01Tosaleinfo(StreamExecutionEnvironment env, String topic,
                                        Properties prop,String topicOut,
                                        String tableName, Map<String,String> joinFieldsDriver,
                                        Set<String> otherFieldsDriver,
                                        Set<String> fieldsHbase, Class<?> hbaseClazz,
                                        Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                              Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_lbpolKafka01Tosaleinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka01>() {
                    @Override
                    public LbpolKafka01 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka01.class);
                    }
                })
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }

    public static void saleinfoTolbpolKafka01(StreamExecutionEnvironment env, String topic,
                                              Properties prop,String topicOut,
                                              String tableName, Map<String,String> joinFieldsDriver,
                                              Set<String> otherFieldsDriver,
                                              Set<String> fieldsHbase, Class<?> hbaseClazz,
                                              Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                              Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_saleinfoTolbpolKafka01");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof T02salesinfok)
                .map(x -> (T02salesinfok) x)
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }





    public static void lbpolKafka02ToBranchinfo(StreamExecutionEnvironment env, String topic,
                                              Properties prop,String topicOut,
                                              String tableName, Map<String,String> joinFieldsDriver,
                                              Set<String> otherFieldsDriver,
                                              Set<String> fieldsHbase, Class<?> hbaseClazz,
                                              Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                                Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_lbpolKafka02ToBranchinfo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka02>() {
                    @Override
                    public LbpolKafka02 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka02.class);
                    }
                })
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }


    public static void lbpolTolbpol(StreamExecutionEnvironment env, String topic,
                                    Properties prop,String topicOut,
                                    String tableName, Map<String,String> joinFieldsDriver,
                                    Set<String> otherFieldsDriver,
                                    Set<String> fieldsHbase, Class<?> hbaseClazz,
                                    Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                    Map<String,String> filterMapHbase){
        prop.setProperty("group.id","lbpolTolbpol");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka03>() {
                    @Override
                    public LbpolKafka03 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka03.class);
                    }
                })
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }


    public static void lbpolToProductRateConfig(StreamExecutionEnvironment env, String topic,
                                                Properties prop,String topicOut,
                                                String tableName, Map<String,String> joinFieldsDriver,
                                                Set<String> otherFieldsDriver,
                                                Set<String> fieldsHbase, Class<?> hbaseClazz,
                                                Class<?> kafkaClazz, Map<String,String> filterMapDriver,
                                                Map<String,String> filterMapHbase){
        prop.setProperty("group.id","JobPremiums_lbpolToProductRateConfigg");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, LbpolKafka04>() {
                    @Override
                    public LbpolKafka04 map(String s) throws Exception {
                        return JSON.parseObject(s,LbpolKafka04.class);
                    }
                })
                .addSink(new JoinInsertKafka(tableName,topicOut,
                        joinFieldsDriver,otherFieldsDriver,
                        fieldsHbase,hbaseClazz,kafkaClazz,filterMapDriver,filterMapHbase));
    }







    public static void premiums(StreamExecutionEnvironment env, String topic, Properties prop){
        //<固定套路
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop
        );
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        SingleOutputStreamOperator<Lcpol> source = env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .filter(new FilterFunction<Lcpol>() {
                    @Override
                    public boolean filter(Lcpol lcpol) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String today = sdf.format(new Date());
                        if(lcpol.getModifydate().equals(today)){
                            return true;
                        }
                        else{
                            return false;
                        }
                    }
                });
        //固定套路>
        source.print();


//todo -------------------------------------------------------------------------------------------------------------------

//    public static void premise(StreamExecutionEnvironment env, String topic, Properties prop){
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                topic, new SimpleStringSchema(), prop
//        );
//
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//
//
//        env.addSource(kafkaConsumer)
//                .map(new TestMapTransformFunc())
//                .filter(x -> x instanceof Lcpol)
//                .map(x -> (Lcpol) x)
//                .keyBy(new KeySelector<Lcpol, Tuple3<String, String, String>>() {
//                    @Override
//                    public Tuple3<String, String, String> getKey(Lcpol lcpol) throws Exception {
//                        return Tuple3.of(lcpol.getGrppolno(), lcpol.getPolno(), lcpol.getInsuredno());
//                    }
//                }).reduce(new ReduceFunction<Lcpol>() {
//                    @Override
//                    public Lcpol reduce(Lcpol lcpol, Lcpol t1) throws Exception {
//                    Lcpol newLc = new Lcpol();
//                    Double pre = Double.valueOf(lcpol.getPrem());
//                    Double suf = Double.valueOf(t1.getPrem());
//                    newLc.setPrem(String.valueOf(pre + suf));
//                    newLc.setPolno(lcpol.getPolno);
//                    return newLc;
//                    }
//                }).map(new MapFunction<Lcpol, Tuple4<String, String, String, String>>() {
//                    @Override
//                    public Tuple4<String, String, String, String> map(Lcpol lcpol) throws Exception {
//                        return Tuple4.of(lcpol.getGrppolno(), lcpol.getPolno(), lcpol.getInsuredno(), lcpol.getPrem());
//                    }
//                });
////        sum.print();
////        sum.addSink(new PremiumsKuduSinkV2());
//    }



//        env.addSource(kafkaConsumer).map(new TestMapTransformFunc())
//                .filter(x ->  x instanceof Lbpol)
//                .map(x -> (Lbpol)x)
//                .keyBy(new KeySelectorPremiums())
//                .timeWindow(Time.seconds(5))
//                .sum("prem").print();


//        SingleOutputStreamOperator<Tuple2<String, Long>> sum = env.addSource(kafkaConsumer)
//                .map(new TestMapTransformFunc())
//                .filter(x -> x instanceof Lcpol)
//                .map(x -> (Lcpol) x)
//                .map(new MapPremiums())
//                .keyBy(0)
//                .sum(1);
//            //sum的另一种形式
////                .reduce((ReduceFunction<Tuple2<Long, Long>>) (t2, t1) -> new Tuple2<>(t1.f0, t2.f1 + t1.f1)) // value做累加
//
////        sum.print();
//        sum.addSink(new PremiumsKuduSink()).setParallelism(1);
////        sum.addSink(new PremiumsHbaseSink());



//        SingleOutputStreamOperator<Tuple4<String, String, String, String>> sum = env.addSource(kafkaConsumer)
//                .map(new TestMapTransformFunc())
//                .filter(x -> x instanceof Lcpol)
//                .map(x -> (Lcpol) x)
//                .keyBy(new KeySelector<Lcpol, Tuple3<String, String, String>>() {
//                    @Override
//                    public Tuple3<String, String, String> getKey(Lcpol lcpol) throws Exception {
//                        return Tuple3.of(lcpol.getGrppolno(), lcpol.getPolno(), lcpol.getInsuredno());
//                    }
//                }).reduce(new ReduceFunction<Lcpol>() {
//                    @Override
//                    public Lcpol reduce(Lcpol lcpol, Lcpol t1) throws Exception {
//                        Lcpol newLc = new Lcpol();
//                        Double pre = Double.valueOf(lcpol.getPrem());
//                        Double suf = Double.valueOf(t1.getPrem());
//                        newLc.setPrem(String.valueOf(pre + suf));
//                        newLc.setPolno(lcpol.getPolno());
//                        return newLc;
//                    }
//                }).map(new MapFunction<Lcpol, Tuple4<String, String, String, String>>() {
//                    @Override
//                    public Tuple4<String, String, String, String> map(Lcpol lcpol) throws Exception {
//                        return Tuple4.of(lcpol.getGrppolno(), lcpol.getPolno(), lcpol.getInsuredno(), lcpol.getPrem());
//                    }
//                });
////        sum.print();
//        sum.addSink(new PremiumsKuduSinkV2());
    }



//    public static void testCK(StreamExecutionEnvironment env, String topic, Properties prop){
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                topic, new SimpleStringSchema(), prop
//        );
//        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        env.addSource(kafkaConsumer)
//                .map(new TestMapTransformFunc())
//                .filter(x -> x instanceof Ldcode)
//                .map(x -> (Ldcode) x)
//                .addSink(new AbstractCKSink<>());
//    }


}
