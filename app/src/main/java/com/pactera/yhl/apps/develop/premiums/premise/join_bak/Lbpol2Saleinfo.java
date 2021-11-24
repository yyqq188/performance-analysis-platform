package com.pactera.yhl.apps.develop.premiums.premise.join_bak;


import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity01;
import com.pactera.yhl.entity.source.Lbpol;
import com.pactera.yhl.entity.source.T02salesinfok;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Lbpol2Saleinfo extends AbstractInsertKafka<Lbpol>{
    //1 要从主流中需要取得的字段 之 关联字段
    //2 要从主流中需要取得的字段 之 其他字段
    //3 要从hbase中取得的字段
    //4 hbase表的实体类名字
    //5 发到kafka的实体类的名字  (固定的变量名)
    //填充到kafka实体类的字段(不需要写),可以从 2 和 3中获得

    String[] joinFieldsDriver = new String[]{};
    String[] otherFieldsDriver = new String[]{};
    String[] fieldsHbase = new String[]{};
    Class<T02salesinfok> hbaseClazz = T02salesinfok.class;
    String hbaseClazzStr = "T02salesinfok.class";
    PremiumsKafkaEntity01 kafkaEntity = new PremiumsKafkaEntity01();
    public Lbpol2Saleinfo(String topic){
        tableName = "KLMIDAPP:t02salesinfok_salesId";//HBase中间表名
        this.topic = topic; //"testyhlv3";  //目的topic
    }

//    @Override
//    public void handle(Lbpol value, Context context, HTable hTable) throws Exception {
//        Map<String,Object> map=new HashMap<String,Object>();
//
//        //todo 关联字段
//        StringBuilder rowkeySb = new StringBuilder();
//        for(String rowkey:joinFieldsDriver){
//            map.put("value",value);
//            String expression = "value.get"+ Util.LargerFirstChar(rowkey)+"()";
//            Object value1 = Util.convertToCode(expression,map);
//            rowkeySb.append(value1);
//        }
//        Result result = Util.getHbaseResultSync(rowkeySb.toString(),hTable);
//
//        //todo 源字段的截取 也就是接下来的需要用的字段
//        Map<String,Object> otherFieldsMap = new HashMap<>();
//        for(String rowkey:otherFieldsDriver){
//            map.put("value",value);
//            String expression = "value.get"+ Util.LargerFirstChar(rowkey)+"()";
//            Object value1 = Util.convertToCode(expression,map);
//            otherFieldsMap.put(rowkey,value1);
//        }
//
//
//        for(Cell cell:result.listCells()){
//            String valueJson = Bytes.toString(CellUtil.cloneValue(cell));
//
//
////            T02salesinfok t02salesinfok = JSON.parseObject(valueJson, T02salesinfok.class);
////            String workarea = t02salesinfok.getWorkarea();
//            //todo 以及中间表的实体类
//            map.put("valueJson",valueJson);
//            map.put(hbaseClazzStr,hbaseClazz);
//            String expression2 = "JSON.parseObject(valueJson, " + hbaseClazzStr +")";
//            Object hbaseClassObj = Util.convertToCode(expression2,map);
//
//            //todo 取要取的字段 也就是需要扩充打宽的字段
//            for(String rowkey:fieldsHbase){
//                map.put("value",hbaseClassObj);
//                String expression3 = "value.get"+ Util.LargerFirstChar(rowkey)+"()";
//                Object value1 = Util.convertToCode(expression3,map);
//                otherFieldsMap.put(rowkey,value1);
//            }
//
//            //premiumsKafkaEntity01.setPrem(prem);
//            //todo 传入新消息的实体类
//            for(String rowkey:otherFieldsMap.keySet()){
//                map.put("kafkaEntity",kafkaEntity);
//                map.put("value",otherFieldsMap.get(rowkey));
//                String expression3 = "kafkaEntity.set"+ Util.LargerFirstChar(rowkey)+"(value)";
//                Object value1 = Util.convertToCode(expression3,map);
//            }
//
//            producer.send(new ProducerRecord<>(topic,
//                    JSON.toJSONString(kafkaEntity)));
//
//        }
//
//
//
//    }

    @Override
    public void handle(Lbpol value, Context context, HTable hTable) throws Exception {
        //todo 关联字段
        String rowkey = value.getAgentcode();
        Result result = Util.getHbaseResultSync(rowkey+"",hTable);
        //todo 源字段的截取 也就是接下来的需要用的字段
        String managecom = value.getManagecom();
        String prem = value.getPrem();
        for(Cell cell:result.listCells()){
            String valueJson = Bytes.toString(CellUtil.cloneValue(cell));
            //todo 取要取的字段 也就是需要扩充打宽的字段
            //todo 以及中间表的实体类
            T02salesinfok t02salesinfok = JSON.parseObject(valueJson, T02salesinfok.class);
            //todo 加过滤
            if("08".equals(t02salesinfok.getChannel_id())){
                String workarea = t02salesinfok.getWorkarea();
                String versionId = t02salesinfok.getVersion_id();
                //
                if(workarea != null && "2021".equals(versionId)){
                    System.out.println("----++++ rowkey "+rowkey);
                    //todo 传入新消息的实体类
                    PremiumsKafkaEntity01 premiumsKafkaEntity01 = new PremiumsKafkaEntity01();
                    premiumsKafkaEntity01.setPrem(prem);
                    premiumsKafkaEntity01.setManagecom(managecom);
                    premiumsKafkaEntity01.setWorkarea(workarea);

                    producer.send(new ProducerRecord<>(topic,
                            JSON.toJSONString(premiumsKafkaEntity01)));
                }


            }


        }

    }





}
