package com.pactera.yhl.apps.measure.join.Utilities;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.Rate2Lpol;
import com.pactera.yhl.apps.measure.entity.Lpol2salesinfo;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/12 14:26
 */
public class Rate2Salesinfo extends AbstractInsertKafka<Rate2Lpol> {
    protected static int i = 1;
    public Rate2Salesinfo(String topic){
        tableName = "KLMIDAPP:T02SALESINFO_K_SALES_ID";//HBase中间表名
        this.topic = topic;
    }
    @Override
    public void handle(Rate2Lpol value, Context context, HTable hTable) throws Exception {
        String agentcode = value.getAgentcode();
        Result salesResult = Util.getHbaseResultSync(agentcode, hTable);
        if (!salesResult.isEmpty()) {
            Lpol2salesinfo lpd2salesinfo = new Lpol2salesinfo();
            for (Cell salesListCell : salesResult.listCells()) {
                JSONObject salesJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(salesListCell)));
                String version_id = salesJsonObject.getString("version_id");
                String channel_id = salesJsonObject.getString("channel_id");
                String stat = salesJsonObject.getString("stat");
                if("08".equalsIgnoreCase(channel_id) && "2021".equalsIgnoreCase(version_id) && "1".equalsIgnoreCase(stat)){
                    String rank = salesJsonObject.getString("rank");

                    lpd2salesinfo.setSales_id(value.getAgentcode());
                    lpd2salesinfo.setTeam_id(salesJsonObject.getString("team_id"));
                    lpd2salesinfo.setRank(rank);
                    lpd2salesinfo.setPrem(value.getPrem());
                }

            }
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lpd2salesinfo)));

            System.out.println(i++);
        }

    }

}
