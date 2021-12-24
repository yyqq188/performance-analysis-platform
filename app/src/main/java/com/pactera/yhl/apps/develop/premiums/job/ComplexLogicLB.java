package com.pactera.yhl.apps.develop.premiums.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.apps.develop.premiums.entity.LbpolKafka03;
import com.pactera.yhl.apps.develop.premiums.entity.LbpolKafka04;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity02;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity03;
import com.pactera.yhl.util.Util;
import net.sf.cglib.beans.BeanCopier;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Method;

public class ComplexLogicLB {
    //这里得到的结果就是要更改他的product_payintv(期缴趸缴) payyears(缴费年期) 还有 productcode

    public static void complexLogic(LbpolKafka03 lbpolKafka03,
                                     String topicInt,String topicOut,
                                    HTable productConfigHbase, HTable midHbase,Producer producer) throws Exception {

        LbpolKafka04 lbpolKafka04 = new LbpolKafka04();
        //拷贝属性
        final BeanCopier beanCopier = BeanCopier.create(LbpolKafka03.class,
                LbpolKafka04.class, false);
        beanCopier.copy(lbpolKafka03,lbpolKafka04,null);


        String contplancode = lbpolKafka03.getContplancode();
        System.out.println("contplancode = " + contplancode);
        String contno = lbpolKafka03.getContno();
        System.out.println("contno = " + contno);
        String polno = lbpolKafka03.getPolno();
        System.out.println("polno = " + polno);
        String mainpolno = lbpolKafka03.getMainpolno();
        System.out.println("mainpolno = " + mainpolno);
        String payyears = lbpolKafka03.getPayyears();
        System.out.println("payyears = " + payyears);
        String payintv = lbpolKafka03.getPayintv();
        System.out.println("payintv = " + payintv);
        String riskcode = lbpolKafka03.getRiskcode();
        System.out.println("riskcode = " + riskcode);

        if (mainpolno.equals(polno)) {
            // 如果相等 是主险 看期趸交 获得主险的期趸交
            String intv_pol = intvOfPol(payintv);
            System.out.println("intv_pol = " + intv_pol);
            String intv_product = joinHbaseProductConfig(contplancode,productConfigHbase);
            System.out.println("intv_product = " + intv_product);
            if (!StringUtils.isBlank(contplancode)) {
                //contplancode --> productcode
                if (intv_product.equals(intv_pol)) {
                    if(intv_product.equals("期缴")){
                        //主线的payyears
                        sendKafkaPremiumsKafkaEntityLB04(producer,topicOut,lbpolKafka04);
                        //发到hbase
                        insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                    }else if(intv_product.equals("趸缴")){
                        lbpolKafka04.setPayyears("0");
                        //主线的payyears
                        sendKafkaPremiumsKafkaEntityLB04(producer,topicOut,lbpolKafka04);
                        //发到hbase
                        insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                    }

                } else if (contplancode.equals("C174") || contplancode.equals("C176")) {
                    //主线的payyears
                    sendKafkaPremiumsKafkaEntityLB04(producer,topicOut,lbpolKafka04);
                    //发到hbase
                    insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                } else {
                    //查hbase
                    boolean succuss = findHbase(contno, midHbase, lbpolKafka04, producer, topicOut);
                    if(succuss){
                        //发到kafka
                        sendKafkaPremiumsKafkaEntityLB03(producer,topicInt,lbpolKafka03);
                    }
                }
            } else {
                if(intv_pol.equals("期缴")){
                    // 把主险的期趸交intv_pol 赋给intv_product    riskcode -> productcode\
                    lbpolKafka04.setContplancode(riskcode);
                    sendKafkaPremiumsKafkaEntityLB04(producer,topicOut,lbpolKafka04);
                    //发到hbase
                    insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                }else if(intv_pol.equals("趸缴")){
                    // 把主险的期趸交intv_pol 赋给intv_product    riskcode -> productcode\
                    lbpolKafka04.setContplancode(riskcode);
                    lbpolKafka04.setPayyears("0");
                    sendKafkaPremiumsKafkaEntityLB04(producer,topicOut,lbpolKafka04);
                    //发到hbase
                    insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                }

            }
        } else {
            //查hbase
            boolean succuss = findHbase(contno, midHbase, lbpolKafka04, producer, topicOut);
            if(succuss){
                //发到kafka
                sendKafkaPremiumsKafkaEntityLB03(producer,topicInt,lbpolKafka03);
            }
        }
    }


    //关联ProductConfig
    private static String joinHbaseProductConfig(String rowkey,HTable hTable) throws Exception {
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = hTable.get(get);
        byte[] value = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("product_config"));
        String s = new String(value);
        String product_payintv = JSON.parseObject(s).get("product_payintv").toString();
        return product_payintv;
    }
    private static String intvOfPol(String payintv){
        if(payintv.equals("12")){
            return "期缴";
        }else if(payintv.equals("0")){
            return "趸缴";
        }
        return null;
    }
    //发送到hbase
    private static void insertIntoHbase(String rowkey,String product_payintv,
                                        String payyears,String contplancode,HTable hTable) throws Exception {
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("product_payintv"),Bytes.toBytes(product_payintv));
        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("payyears"),Bytes.toBytes(payyears));
        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("contplancode"),Bytes.toBytes(contplancode));
        hTable.put(put);
    }
    //查hbase
    private static boolean findHbase(String rowkey,HTable hTable,
                                  LbpolKafka04 lbpolKafka04,Producer producer,String topic) throws Exception{
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = hTable.get(get);
        if(result.listCells() == null) return true;
        for(Cell cell:result.listCells()){
            String field = new String(CellUtil.cloneQualifier(cell));
            String value =new String(CellUtil.cloneValue(cell));
            setNewStringValue(field,lbpolKafka04,value);

        }
        sendKafkaPremiumsKafkaEntityLB04(producer,topic,lbpolKafka04);
        return false;
    }

    private static void sendKafkaPremiumsKafkaEntityLB03(Producer producer,
                                                       String topic,
                                                       LbpolKafka03 lbpolKafka03){
        producer.send(new ProducerRecord<>(topic,
                JSON.toJSONString(lbpolKafka03,
                        SerializerFeature.WriteMapNullValue,
                        SerializerFeature.DisableCircularReferenceDetect,
                        SerializerFeature.WriteDateUseDateFormat)));
    }
    private static void sendKafkaPremiumsKafkaEntityLB04(Producer producer,
                                                       String topic,
                                                       LbpolKafka04 lbpolKafka04){
        producer.send(new ProducerRecord<>(topic,
                JSON.toJSONString(lbpolKafka04,
                        SerializerFeature.WriteMapNullValue,
                        SerializerFeature.DisableCircularReferenceDetect,
                        SerializerFeature.WriteDateUseDateFormat)));
    }

    private static LbpolKafka04 setNewStringValue(String fieleName,LbpolKafka04 lbpolKafka04,String newValue) throws Exception {
        String methodName = "set"+ Util.LargerFirstChar(fieleName);
        Method method = lbpolKafka04.getClass().getDeclaredMethod(methodName, String.class);
        method.invoke(lbpolKafka04,Util.toString(newValue));
        return lbpolKafka04;
    }



}
