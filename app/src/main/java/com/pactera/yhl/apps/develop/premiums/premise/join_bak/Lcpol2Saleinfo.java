package com.pactera.yhl.apps.develop.premiums.premise.join_bak;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.entity.source.T02salesinfok;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

public class Lcpol2Saleinfo extends AbstractInsertKafka<Lcpol>{
    //1 要从主流中需要取得的字段 之 关联字段
    //2 要从主流中需要取得的字段 之 其他字段
    //3 要从hbase中取得的字段
    //4 hbase表的实体类名字
    //5 发到kafka的实体类的名字  (固定的变量名)
    //填充到kafka实体类的字段(不需要写),可以从 2 和 3中获得

//    String[] joinFieldsDriver;
//    String[] otherFieldsDriver;
//    String[] fieldsHbase;
//    Class<T02salesinfok> hbaseClazz = T02salesinfok.class;
//    String hbaseClazzStr = "T02salesinfok.class";
//    PremiumsKafkaEntity01 kafkaEntity = new PremiumsKafkaEntity01();


    public Lcpol2Saleinfo(String tableName, String topic,
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
    public void handle(Lcpol value, Context context, HTable hTable) throws Exception {

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
