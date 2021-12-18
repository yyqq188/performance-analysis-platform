package com.pactera.yhl.apps.measure.join.Lcpol;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/25 19:13
 */
public class Lcopl2Product extends AbstractInsertKafka<Lcpol> {
    protected static int i = 1;
    public Lcopl2Product(String topic){
        tableName = "kl_base:product_config";//HBase中间表名
        this.topic = topic;
    }
    @Override
    public void handle(Lcpol value, Context context, HTable hTable) throws Exception {
        String contplancode = value.getContplancode();
        Result result = Util.getHbaseResultSync(contplancode, hTable);
        if (!result.isEmpty()) {
            for (Cell listCell : result.listCells()) {
                //缴费方式
                String payType = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell))).getString("product_payintv");
                if("期缴".equalsIgnoreCase(payType)){
                    //缴费年限
                    String payIntv = value.getPayintv();
                    //缴费年期
                    int payYears;
                    if(StringUtils.isNotBlank(value.getPayendyear())){
                        payYears = Integer.parseInt(value.getPayendyear());
                    }else {
                        payYears = 0;
                    }
                    if("0".equalsIgnoreCase(payIntv)){
                        value.setPayendyear("0");
                    }else if ("12".equalsIgnoreCase(payIntv) && 10 <= payYears){
                        value.setPayendyear("10");
                    }
                }
            }
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(value)));

            System.out.println(Lcopl2Product.i++);
        }

    }

}
