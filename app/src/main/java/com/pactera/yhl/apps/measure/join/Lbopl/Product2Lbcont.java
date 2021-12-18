package com.pactera.yhl.apps.measure.join.Lbopl;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
import com.pactera.yhl.entity.source.Lbpol;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Product2Lbcont extends AbstractInsertKafka<Lbpol> {
    protected static int i = 1;
    public Product2Lbcont(String topic){
        tableName = "KLMIDAPP:LBCONT_CONTNO";//HBase中间表名
        this.topic = topic;
    }
    @Override
    public void handle(Lbpol value, Context context, HTable hTable) throws Exception {
        String contno = value.getContno();
        Result result = Util.getHbaseResultSync(contno, hTable);
        if (!result.isEmpty()) {
            for (Cell listCell : result.listCells()) {
                String signdate = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell))).getString("signdate");
                //取保单签单日期
                value.setSigndate(signdate);
            }
        }
        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(value)));

        System.out.println(i++);
    }
}
