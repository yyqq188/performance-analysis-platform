package com.pactera.yhl.apps.measure.join.Lbopl;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
import com.pactera.yhl.entity.source.Lbpol;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Lbpol2Product extends AbstractInsertKafka<Lbpol> {
    protected static int i = 1;
    public Lbpol2Product(String topic) {
        tableName = "kl_base:product_config";//HBase中间表名
        this.topic = topic;
    }
    @Override
    public void handle(Lbpol value, Context context, HTable hTable) throws Exception {
        String contplancode = value.getContplancode();
        Result result = Util.getHbaseResultSync(contplancode, hTable);
        if (!result.isEmpty()) {
            for (Cell listCell : result.listCells()) {
                String product_payintv = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell))).getString("product_payintv");
                if("期缴".equalsIgnoreCase(product_payintv)){
                    //缴费方式
                    int payintv;
                    if(StringUtils.isNotBlank(value.getPayintv())){
                        payintv = Integer.parseInt(value.getPayintv());
                    }else {
                        payintv = 0;
                    }
                    if(0 == payintv){
                        value.setPayendyear("0");
                    }else if (10 <= payintv){
                        value.setPayendyear("10");
                    }
                }
            }
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(value)));
            System.out.println(i++);
        }
    }

}
