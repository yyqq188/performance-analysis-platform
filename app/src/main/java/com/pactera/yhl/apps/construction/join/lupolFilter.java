package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author: TSY
 * @create: 2021/11/25 0025 下午 20:28
 * @description:   读取期趸缴配置表，所取保费为期交
 */
public class lupolFilter extends AbstractInsertKafka<LupFilter> {

    public lupolFilter(String topic) {
        tableName = "kl_base:product_config";//HBase中间表名
        this.topic = topic;//目的topic
    }

    @Override
    public void handle(LupFilter lupfilter, Context context, HTable hTable) throws Exception {

        String contplancode = lupfilter.getContplancode();
        System.out.println("contplancode"+contplancode);
        Result configureResult = Util.getHbaseResultSync(contplancode + "", hTable);
        if (!configureResult.isEmpty()) {
            for (Cell listCell : configureResult.listCells()) {
                String value = Bytes.toString(CellUtil.cloneValue(listCell));
                System.out.println("所取保费为期缴： " + value);
                //todo 期缴
                if("期缴".equals(JSONObject.parseObject(value).getString("product_payintv"))){
                    //System.out.println("产品代码-期缴：：：" + JSONObject.parseObject(value).getString("product_payintv"));
                    producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lupfilter)));
                }
            }
        }
    }
}
