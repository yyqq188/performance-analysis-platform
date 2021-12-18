package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.apps.construction.util.DateUDF;
import com.pactera.yhl.apps.construction.util.Utils;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author: TSY
 * @create: 2021/11/26 0026 上午 9:45
 * @description:   关联Lccont表，取签单日期
 */
public class lupolcFilterSigncom extends AbstractInsertKafka<LupFilter> {

    public lupolcFilterSigncom(String topic) {
//        tableName = "KLMIDAPP:LCCONT_CONTNO";//HBase中间表名
        tableName = "KLHBASE:LCCONT_INS";//HBase中间表名
        this.topic = topic;//目的topic
    }

    @Override
    public void handle(LupFilter lupfilter, Context context, HTable hTable) throws Exception {

        String contno = lupfilter.getContno();

        String signdate = Util.getHbaseColumnNameValue(hTable, contno, "f", "signdate");
        System.out.println("hbaseColumnNameValue::"+signdate);

        if(signdate.length() != 0){
            String firstDayOfMonth = DateUDF.getFirstDayOfMonth();
            String endDayOfMonth = DateUDF.getEndDayOfMonth();
            String substring = signdate.substring(0, 10);
            if(substring.compareTo(firstDayOfMonth) >= 0 && substring.compareTo(endDayOfMonth) <= 0) {
                lupfilter.setSigndate(substring);
                producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lupfilter)));
            }
        }
    }
}
