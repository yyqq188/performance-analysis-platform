package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.entity.source.Lbpol;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author: TSY
 * @create: 2021/12/3 0003 上午 9:33
 * @description:  lbpol去lbpol获取主险产品代码和缴费年期
 */
public class LbpolFilterLbcont extends AbstractInsertKafka<LupFilter> {

    public LbpolFilterLbcont(String topic) {
        tableName = "KLMIDAPP:LBPOL_CONTNO";//HBase中间表名
        this.topic = topic; //目的topic
    }
    private static LupFilter lbpFilterlbp = new LupFilter();

    @Override
    public void handle(LupFilter lupfilter, Context context, HTable hTable) throws Exception {
        System.out.println("lbpol:::"+lupfilter);
        String contplancode = lupfilter.getContplancode();
        if (StringUtils.isBlank(contplancode)) {
            String contno = lupfilter.getContno();
            double payIntv = Double.parseDouble(lupfilter.getPayintv());//交费间隔
            double payEndYear = Double.parseDouble(lupfilter.getPayendyear());//终交年龄年期
            double insuYear = Double.parseDouble(lupfilter.getInsuyear());//保险年龄年期
            Result lbpResult = Util.getHbaseResultSync(contno + "", hTable);
            if (!lbpResult.isEmpty()) {
                //保单下所有险种集合
                Map<String, Lbpol> map = new HashMap<>();
                Lbpol lbpol;
                for (Cell listCell : lbpResult.listCells()) {
                    lbpol = JSONObject.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)), Lbpol.class);
                    if(lbpol.getPolno().equalsIgnoreCase(lbpol.getMainpolno())){
                        map.put(lbpol.getPolno(),lbpol);
                    }
                }
                Set<String> keys = map.keySet();
                for (String key : keys) {
                    lbpol = map.get(key);
                    //开窗
                    // order by PayIntv desc,PayEndYear ,InsuYear,PayEndYearFlag,InsuYearFlag
                    if(payIntv < Double.parseDouble(lbpol.getPayintv()) && payEndYear > Double.parseDouble(lbpol.getPayendyear())
                            && insuYear > Double.parseDouble(lbpol.getInsuyear())) {
                        //去主险的险种号做产品组合代码
                        lupfilter.setContplancode(lbpol.getRiskcode());
                    }
                }
            }else {
                lupfilter.setContplancode(lupfilter.getRiskcode());
            }
            lbpFilterlbp.setPolno(lupfilter.getPolno());
            lbpFilterlbp.setEdorno(lupfilter.getEdorno());
            lbpFilterlbp.setContno(lupfilter.getContno());
            lbpFilterlbp.setContplancode(lupfilter.getContplancode());
            lbpFilterlbp.setMainpolno(lupfilter.getMainpolno());
            lbpFilterlbp.setRiskcode(lupfilter.getRiskcode());
            lbpFilterlbp.setPayendyear(lupfilter.getPayendyear());
            lbpFilterlbp.setSigndate(lupfilter.getSigndate());
            lbpFilterlbp.setAgentcode(lupfilter.getAgentcode());
            lbpFilterlbp.setPrem(lupfilter.getPrem());
            lbpFilterlbp.setMark("lbpol");
        }else {
            lbpFilterlbp.setPolno(lupfilter.getPolno());
            lbpFilterlbp.setEdorno(lupfilter.getEdorno());
            lbpFilterlbp.setContno(lupfilter.getContno());
            lbpFilterlbp.setContplancode(lupfilter.getContplancode());
            lbpFilterlbp.setMainpolno(lupfilter.getMainpolno());
            lbpFilterlbp.setRiskcode(lupfilter.getRiskcode());
            lbpFilterlbp.setPayendyear(lupfilter.getPayendyear());
            lbpFilterlbp.setSigndate(lupfilter.getSigndate());
            lbpFilterlbp.setAgentcode(lupfilter.getAgentcode());
            lbpFilterlbp.setPrem(lupfilter.getPrem());
            lbpFilterlbp.setMark("lbpol");
        }

        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lbpFilterlbp)));

        lbpFilterlbp.setPolno(null);
        lbpFilterlbp.setEdorno(null);
        lbpFilterlbp.setContno(null);
        lbpFilterlbp.setSigndate(null);
        lbpFilterlbp.setContplancode(null);
        lbpFilterlbp.setMainpolno(null);
        lbpFilterlbp.setRiskcode(null);
        lbpFilterlbp.setPayendyear(null);
        lbpFilterlbp.setSigndate(null);
        lbpFilterlbp.setAgentcode(null);
        lbpFilterlbp.setPrem(null);

    }
}