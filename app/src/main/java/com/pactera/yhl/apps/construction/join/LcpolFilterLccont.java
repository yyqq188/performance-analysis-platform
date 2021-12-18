package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.entity.source.Lcpol;
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
 * @create: 2021/12/3 0003 上午 9:29
 * @description:   lcpol去lcpol获取主险产品代码和缴费年期
 */
public class LcpolFilterLccont extends AbstractInsertKafka<LupFilter> {

    public LcpolFilterLccont(String topic) {
        tableName = "KLMIDAPP:LCPOL_CONTNO";//HBase中间表名
        this.topic = topic; //目的topic
    }
    private static LupFilter lcpFilterlcp = new LupFilter();
    @Override
    public void handle(LupFilter lupfilter, Context context, HTable hTable) throws Exception {
        //System.out.println("lcpol:::"+lupfilter);
        String contplancode = lupfilter.getContplancode();
        //判断产品组合代码是否为空,为空取主险的险种号
        if (StringUtils.isBlank(contplancode)) {
            String contno = lupfilter.getContno();
            double payIntv = Double.parseDouble(lupfilter.getPayintv());//交费间隔
            double payEndYear = Double.parseDouble(lupfilter.getPayendyear());//终交年龄年期
            double insuYear = Double.parseDouble(lupfilter.getInsuyear());//保险年龄年期
            Result lcpResult = Util.getHbaseResultSync(contno + "", hTable);
            if (!lcpResult.isEmpty()) {
                //保单下所有险种集合
                Map<String, Lcpol> map = new HashMap<>();
                Lcpol lcpol;
                for (Cell listCell : lcpResult.listCells()) {
                    lcpol = JSONObject.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)), Lcpol.class);
                    if(lcpol.getPolno().equalsIgnoreCase(lcpol.getMainpolno())){
                        map.put(lcpol.getPolno(),lcpol);
                    }
                }
                Set<String> keys = map.keySet();
                for (String key : keys) {
                    lcpol = map.get(key);
                    //开窗
                    // order by PayIntv desc,PayEndYear ,InsuYear,PayEndYearFlag,InsuYearFlag
                    if(payIntv < Double.parseDouble(lcpol.getPayintv()) && payEndYear > Double.parseDouble(lcpol.getPayendyear())
                            && insuYear > Double.parseDouble(lcpol.getInsuyear())){
                        //去主险的险种号做产品组合代码
                        lupfilter.setContplancode(lcpol.getRiskcode());
                    }
                }
            }else {
                lupfilter.setContplancode(lupfilter.getRiskcode());
            }
            lcpFilterlcp.setPolno(lupfilter.getPolno());
            lcpFilterlcp.setEdorno(null);
            lcpFilterlcp.setContno(lupfilter.getContno());
            lcpFilterlcp.setContplancode(lupfilter.getContplancode());
            lcpFilterlcp.setMainpolno(lupfilter.getMainpolno());
            lcpFilterlcp.setRiskcode(lupfilter.getRiskcode());
            lcpFilterlcp.setPayendyear(lupfilter.getPayendyear());
            lcpFilterlcp.setSigndate(lupfilter.getSigndate());
            lcpFilterlcp.setAgentcode(lupfilter.getAgentcode());
            lcpFilterlcp.setPrem(lupfilter.getPrem());
            lcpFilterlcp.setMark("lcpol");
        }else {
            lcpFilterlcp.setPolno(lupfilter.getPolno());
            lcpFilterlcp.setEdorno(null);
            lcpFilterlcp.setContno(lupfilter.getContno());
            lcpFilterlcp.setContplancode(lupfilter.getContplancode());
            lcpFilterlcp.setMainpolno(lupfilter.getMainpolno());
            lcpFilterlcp.setRiskcode(lupfilter.getRiskcode());
            lcpFilterlcp.setPayendyear(lupfilter.getPayendyear());
            lcpFilterlcp.setSigndate(lupfilter.getSigndate());
            lcpFilterlcp.setAgentcode(lupfilter.getAgentcode());
            lcpFilterlcp.setPrem(lupfilter.getPrem());
            lcpFilterlcp.setMark("lcpol");
        }

        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lcpFilterlcp)));

        lcpFilterlcp.setPolno(null);
        lcpFilterlcp.setContno(null);
        lcpFilterlcp.setContplancode(null);
        lcpFilterlcp.setMainpolno(null);
        lcpFilterlcp.setSigndate(null);
        lcpFilterlcp.setRiskcode(null);
        lcpFilterlcp.setPayendyear(null);
        lcpFilterlcp.setAgentcode(null);
        lcpFilterlcp.setPrem(null);

    }
}