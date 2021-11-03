package com.pactera.yhl.apps.develop.premiums.premise.join;


import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity01;
import com.pactera.yhl.entity.source.Lbpol;
import com.pactera.yhl.entity.source.T02salesinfok;
import com.pactera.yhl.util.Util;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Lbpol2Saleinfo extends AbstractInsertKafka<Lbpol>{
    public Lbpol2Saleinfo(){

        tableName = "KLMIDAPP:t02salesinfok_salesId";//HBase中间表名
        topic = "t1";  //目的topic
    }
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
            String workarea = t02salesinfok.getWorkarea();
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
