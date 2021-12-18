package com.pactera.yhl.apps.measure.join.Lbopl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;

import com.pactera.yhl.entity.source.Lbpol;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;

/**
 * @author Sun Haitian
 * @Description  处理源
 * @create 2021/11/12 10:51
 */
public class Lbpol2Lpedoritem extends AbstractInsertKafka<Lbpol> {
    protected static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    protected static int i = 1;

    public Lbpol2Lpedoritem(String topic){
        tableName = "KLMIDAPP:LPEDORITEM_CONTNO_EDORNO";//HBase中间表名
        this.topic = topic;
    }

    @Override
    public void handle(Lbpol value, Context context, HTable hTable) throws Exception {
//        if (sdf.format(System.currentTimeMillis()).equalsIgnoreCase(sdf.format(sdf.parse(value.getOp_ts())))) {
        String salechnl = value.getSalechnl();
        String edorno = value.getEdorno();
        //仅限 银保渠道 , 剔除 个单/续保 , 保全号不能为续保类型 edorno != 'XB%'
        if(("04".equalsIgnoreCase(salechnl) || "13".equalsIgnoreCase(salechnl))
                && "0".equalsIgnoreCase(value.getRenewcount())
                && "1".equalsIgnoreCase(value.getConttype())
                && !"XB".equalsIgnoreCase(edorno.substring(0,2))){
            String contno = value.getContno();
            //关联保全表判断退保类型
            Result lpdResult = Util.getHbaseResultSync(contno + edorno, hTable);
            if (!lpdResult.isEmpty()){
                for (Cell lpdListCell : lpdResult.listCells()) {
                    JSONObject lpdJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(lpdListCell)));
                    String edortype = lpdJsonObject.getString("edortype");
                    //限定状态为 犹退或当日撤单 且 保全状态完成
                    if(("WT".equalsIgnoreCase(edortype) && "0".equalsIgnoreCase(lpdJsonObject.getString("edorstate"))) || "YBT".equalsIgnoreCase(edorno.substring(0,3))){
                        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(value)));

                        System.out.println(i++);
                    }
                }
            }
        }
//        }

    }

}
