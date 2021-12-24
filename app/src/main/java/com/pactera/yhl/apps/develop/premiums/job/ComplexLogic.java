package com.pactera.yhl.apps.develop.premiums.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity02;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity03;
import com.pactera.yhl.util.Util;
import net.sf.cglib.beans.BeanCopier;
import net.sf.cglib.core.Converter;
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

import java.io.IOException;
import java.lang.reflect.Method;

public class ComplexLogic {
    //这里得到的结果就是要更改他的product_payintv(期缴趸缴) payyears(缴费年期) 还有 productcode

    public static void complexLogic(PremiumsKafkaEntity02 premiumsKafkaEntity02,
                                     String topicInt,String topicOut,
                                    HTable productConfigHbase, HTable midHbase,Producer producer) throws Exception {

        PremiumsKafkaEntity03 premiumsKafkaEntity03 = new PremiumsKafkaEntity03();
        //拷贝属性
        final BeanCopier beanCopier = BeanCopier.create(PremiumsKafkaEntity02.class,
                PremiumsKafkaEntity03.class, false);
        beanCopier.copy(premiumsKafkaEntity02,premiumsKafkaEntity03,null);


        String contplancode = premiumsKafkaEntity02.getContplancode();
        System.out.println("contplancode = " + contplancode);
        String contno = premiumsKafkaEntity02.getContno();
        System.out.println("contno = " + contno);
        String polno = premiumsKafkaEntity02.getPolno();
        System.out.println("polno = " + polno);
        String mainpolno = premiumsKafkaEntity02.getMainpolno();
        System.out.println("mainpolno = " + mainpolno);
        String payyears = premiumsKafkaEntity02.getPayyears();
        System.out.println("payyears = " + payyears);
        String payintv = premiumsKafkaEntity02.getPayintv();
        System.out.println("payintv = " + payintv);
        String riskcode = premiumsKafkaEntity02.getRiskcode();
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
                        sendKafkaPremiumsKafkaEntity03(producer,topicOut,premiumsKafkaEntity03);
                        //发到hbase
                        insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                    }else if(intv_product.equals("趸缴")){
                        premiumsKafkaEntity03.setPayyears("0");
                        //主线的payyears
                        sendKafkaPremiumsKafkaEntity03(producer,topicOut,premiumsKafkaEntity03);
                        //发到hbase
                        insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                    }

                } else if (contplancode.equals("C174") || contplancode.equals("C176")) {
                    //主线的payyears
                    sendKafkaPremiumsKafkaEntity03(producer,topicOut,premiumsKafkaEntity03);
                    //发到hbase
                    insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                } else {
                    //查hbase
                    boolean succuss = findHbase(contno, midHbase, premiumsKafkaEntity03, producer, topicOut);
                    if(succuss){
                        //发到kafka
                        sendKafkaPremiumsKafkaEntity02(producer,topicInt,premiumsKafkaEntity02);
                    }
                }
            } else {
                if(intv_pol.equals("期缴")){
                    // 把主险的期趸交intv_pol 赋给intv_product    riskcode -> productcode\
                    premiumsKafkaEntity03.setContplancode(riskcode);
                    sendKafkaPremiumsKafkaEntity03(producer,topicOut,premiumsKafkaEntity03);
                    //发到hbase
                    insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                }else if(intv_pol.equals("趸缴")){
                    // 把主险的期趸交intv_pol 赋给intv_product    riskcode -> productcode\
                    premiumsKafkaEntity03.setContplancode(riskcode);
                    premiumsKafkaEntity03.setPayyears("0");
                    sendKafkaPremiumsKafkaEntity03(producer,topicOut,premiumsKafkaEntity03);
                    //发到hbase
                    insertIntoHbase(contno,intv_product,payyears,contplancode,midHbase);
                }

            }
        } else {
            //查hbase
            boolean succuss = findHbase(contno, midHbase, premiumsKafkaEntity03, producer, topicOut);
            if(succuss){
                //发到kafka
                sendKafkaPremiumsKafkaEntity02(producer,topicInt,premiumsKafkaEntity02);
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
                                  PremiumsKafkaEntity03 premiumsKafkaEntity03,Producer producer,String topic) throws Exception{
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = hTable.get(get);
        if(result.listCells() == null) return true;
        for(Cell cell:result.listCells()){
            String field = new String(CellUtil.cloneQualifier(cell));
            String value =new String(CellUtil.cloneValue(cell));
            setNewStringValue(field,premiumsKafkaEntity03,value);

        }
        sendKafkaPremiumsKafkaEntity03(producer,topic,premiumsKafkaEntity03);
        return false;
    }

    private static void sendKafkaPremiumsKafkaEntity02(Producer producer,
                                                       String topic,
                                                       PremiumsKafkaEntity02 premiumsKafkaEntity02){
        producer.send(new ProducerRecord<>(topic,
                JSON.toJSONString(premiumsKafkaEntity02,
                        SerializerFeature.WriteMapNullValue,
                        SerializerFeature.DisableCircularReferenceDetect,
                        SerializerFeature.WriteDateUseDateFormat)));
    }
    private static void sendKafkaPremiumsKafkaEntity03(Producer producer,
                                                       String topic,
                                                       PremiumsKafkaEntity03 premiumsKafkaEntity03){
        producer.send(new ProducerRecord<>(topic,
                JSON.toJSONString(premiumsKafkaEntity03,
                        SerializerFeature.WriteMapNullValue,
                        SerializerFeature.DisableCircularReferenceDetect,
                        SerializerFeature.WriteDateUseDateFormat)));
    }

    private static PremiumsKafkaEntity03 setNewStringValue(String fieleName,PremiumsKafkaEntity03 premiumsKafkaEntity03,String newValue) throws Exception {
        String methodName = "set"+ Util.LargerFirstChar(fieleName);
        Method method = premiumsKafkaEntity03.getClass().getDeclaredMethod(methodName, String.class);
        method.invoke(premiumsKafkaEntity03,Util.toString(newValue));
        return premiumsKafkaEntity03;
    }



}
