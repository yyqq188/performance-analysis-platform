package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.entity.source.Lpedoritem;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author: TSY
 * @create: 2021/11/16 0016 下午 14:22
 * @description:  关联LPEDORITEM保全表，保单状态为当日撤单或者犹豫期退保和状态为0
 */
public class lupol2Lpedoritem extends AbstractInsertKafka<LupFilter> {

    public lupol2Lpedoritem(String topic){
        tableName = "KLMIDAPP:LPEDORITEM_EDORNO_CONTNO";//HBase中间表名
        this.topic = topic;//目的topic
    }
    private static Lpedoritem lpe = null;
    @Override
    public void handle(LupFilter lupfilter, Context context, HTable hTable) throws Exception {
        //判断为lbpol
        if ("lbpol".equals(lupfilter.getMark())) {
            String edorno = lupfilter.getEdorno();
            String contno = lupfilter.getContno();
            //System.out.println("edorno + contno :::"+edorno + contno);
            //todo 过滤lbp.edorno like 'YBT%'为当日撤单
            if ("YBT".equalsIgnoreCase(edorno.substring(0, 3))) {
                producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lupfilter)));
            }else {
                //获取lpedoritem中间表数据
                Result resultLpe = Util.getHbaseResultSync(edorno + contno + "", hTable);
                if (!resultLpe.isEmpty()) {
                    for (Cell cell : resultLpe.listCells()) {
                        String valueJson = Bytes.toString(CellUtil.cloneValue(cell));
                        lpe = JSON.parseObject(valueJson, Lpedoritem.class);
                        //todo lp.EDORTYPE='WT' and  过滤lp.EDORSTATE='0'--犹豫期退保
                        if ("WT".equalsIgnoreCase(lpe.getEdortype()) && "0".equals(lpe.getEdorstate())) {
                            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lupfilter)));
                        }
                    }
                }
            }
        }else {
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lupfilter)));
        }
    }
}
