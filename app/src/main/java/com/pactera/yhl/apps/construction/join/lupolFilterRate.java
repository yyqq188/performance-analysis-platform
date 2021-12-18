package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author: TSY
 * @create: 2021/11/26 0026 上午 10:42
 * @description:  关联产品率值表，取率值
 */
public class lupolFilterRate extends AbstractInsertKafka<LupFilter> {

    public lupolFilterRate(String topic) {
        tableName = "kl_base:product_rate_config";//HBase中间表名
        this.topic = topic;//目的topic
    }

    @Override
    public void handle(LupFilter lupfilter, Context context, HTable hTable) throws Exception {

        String contplancode = lupfilter.getContplancode();
        String payendyear = lupfilter.getPayendyear();

        Result result = Util.getHbaseResultSync(contplancode + payendyear, hTable);
        //todo 产品率值表中有，则取表中率值
        if(!result.isEmpty()){
            for (Cell listCell : result.listCells()) {
                JSONObject jsonObject = JSONObject.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                String state = jsonObject.getString("state");
                //todo 判断状态state为1
                if("1".equals(state)){
                    String start_date = jsonObject.getString("start_date");
                    String end_date = jsonObject.getString("end_date");
                    String signdate = lupfilter.getSigndate();
                    //todo 判断签单日期在区间内
                    if(signdate.compareTo(start_date) >= 0 && signdate.compareTo(end_date) <= 0){
                        Double rate = jsonObject.getDouble("rate");
                        System.out.println("率值rate：  "+rate);
                        Double prem = Double.valueOf(lupfilter.getPrem());
                        lupfilter.setPrem(String.valueOf(prem * rate));
                        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lupfilter)));
                    }
                }
            }
        }else {
            //todo 产品率值表中没有，直接输出
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lupfilter)));
        }
    }
}
