package com.pactera.yhl.apps.measure.join.Lcpol;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.SunUtils;
import com.pactera.yhl.apps.measure.entity.Rate2Lpol;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Sun Haitian
 * @Description 查询折扣率
 * @create 2021/11/26 09:32
 */
public class Lccont2Rate extends AbstractInsertKafka<Lcpol> {
    protected static int i = 1;
    public Lccont2Rate(String topic){
        tableName = "kl_base:product_rate_config";//HBase中间表名
        this.topic = topic;
    }

    @Override
    public void handle(Lcpol value, Context context, HTable hTable) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String contplancode = value.getContplancode();
        String payendyear = value.getPayendyear();
        double rate = 0.0;
        Result result = Util.getHbaseResultSync(contplancode + payendyear, hTable);
        if (!result.isEmpty()) {
            Date signdate = sdf.parse(value.getSigndate());
            for (Cell listCell : result.listCells()) {
                //列名 start_date + end_date
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(listCell));
                Date start_date = sdf.parse(qualifier.substring(0, 10));
                Date end_date =  sdf.parse(qualifier.substring(10));
                if(signdate.after(start_date) && signdate.before(end_date)){
                    JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                    rate = SunUtils.isDoubleNotNull(jsonObject.getString("rate"));
                }
            }
        }else {
            rate = 1.0;
        }
        Rate2Lpol rate2Lpol = new Rate2Lpol();
        rate2Lpol.setAgentcode(value.getAgentcode());
        rate2Lpol.setPrem(rate * SunUtils.isDoubleNotNull(value.getPrem()));
        producer.send(new ProducerRecord<>(topic,
                JSON.toJSONString(rate2Lpol)));

        System.out.println(i++);
    }
}
