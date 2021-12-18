package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.apps.construction.entity.LupToT02;
import com.pactera.yhl.entity.source.T02salesinfok;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * @author: TSY
 * @create: 2021/11/11 0011 下午 19:58
 * @description:    关联人员表
 */
public class lupol2T02salesinfo_k extends AbstractInsertKafka<LupFilter> {

    public lupol2T02salesinfo_k(String topic) {
        tableName = "KLMIDAPP:T02SALESINFO_K_SALES_ID";//HBase中间表名
        this.topic = topic;//目的topic
    }

    private static T02salesinfok t02salesinfok = null;
    private static LupToT02 lutt = new LupToT02();

    @Override
    public void handle(LupFilter lupfilter, Context context, HTable hTable) throws Exception {

        String agentcode = lupfilter.getAgentcode();
        String mark = lupfilter.getMark();
        //获取T02中间表数据
        Result resultT02 = Util.getHbaseResultSync(agentcode + "", hTable);
        if (!resultT02.isEmpty()) {
            for (Cell cell : resultT02.listCells()) {
                String valueJson = Bytes.toString(CellUtil.cloneValue(cell));
                ///System.out.println("t02JSON:" +valueJson);
                t02salesinfok = JSON.parseObject(valueJson, T02salesinfok.class);

                //todo 过滤出银保渠道
                if ("08".equals(t02salesinfok.getChannel_id())) {
                    //todo 2021年
                    if ("2021".equals(t02salesinfok.getVersion_id())) {
                        String prem = lupfilter.getPrem();
                        String branch_id = t02salesinfok.getBranch_id();
                        String sales_id = t02salesinfok.getSales_id();
                        String probation_date = t02salesinfok.getProbation_date().substring(0, 10);

                        System.out.println(prem+" "+branch_id+" "+sales_id+" "+probation_date);

                        lutt.setBranch_id(branch_id);//机构
                        lutt.setSales_id(sales_id);//人员ID
                        if("lbpol".equals(mark)){
                            lutt.setPrem("-" + prem);//总保费
                        }else {
                            lutt.setPrem(prem);//总保费
                        }
                        lutt.setProbation_date(probation_date);//签约日期
                        lutt.setSigndate(lupfilter.getSigndate());//签单日期
                        lutt.setMark(lupfilter.getMark());
                        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lutt)));
                        lutt.setBranch_id(null);//机构
                        lutt.setSales_id(null);//人员ID
                        lutt.setPrem(null);//总保费
                        lutt.setProbation_date(null);//签约日期
                        lutt.setSigndate(null);//签单日期
                        lutt.setMark(null);
                    }
                }
            }
        }
    }
}
