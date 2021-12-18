package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.entity.source.Lcpol;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * @author: TSY
 * @create: 2021/11/19 0019 上午 10:25
 * @description:  过滤出期缴保费
 */
public class lcpolFilter extends AbstractInsertKafka<Lcpol> {

    public lcpolFilter(String topic) {
        tableName = "KLMIDAPP:LPEDORITEM_EDORNO_CONTNO";//HBase中间表名
        this.topic = topic;//目的topic
    }

    private static LupFilter lcpFilter1 = new LupFilter();

    @Override
    public void handle(Lcpol lcpol, Context context, HTable hTable) throws Exception {
        String salechnl = lcpol.getSalechnl();
        String renewcount = lcpol.getRenewcount();
        String conttype = lcpol.getConttype();
        String stateflag = lcpol.getStateflag();
        //todo 保单卡银保渠道
        if ("04".equals(salechnl) || "13".equals(salechnl)) {
            //todo lup.RenewCount=0 --剔除续保and lup.ContType = '1' --1为个单
            if ("0".equals(renewcount) && "1".equals(conttype)) {
                //todo 投保单 statetlag != 0
                if(!"0".equals(stateflag)) {
                    lcpFilter1.setPolno(lcpol.getPolno());
                    lcpFilter1.setEdorno(null);
                    lcpFilter1.setContno(lcpol.getContno());
                    lcpFilter1.setContplancode(lcpol.getContplancode());
                    lcpFilter1.setMainpolno(lcpol.getMainpolno());
                    lcpFilter1.setRiskcode(lcpol.getRiskcode());
                    lcpFilter1.setPayendyear(lcpol.getPayendyear());
                    lcpFilter1.setSigndate("");
                    lcpFilter1.setAgentcode(lcpol.getAgentcode());
                    lcpFilter1.setPrem(lcpol.getPrem());
                    lcpFilter1.setMark("lcpol");

                    lcpFilter1.setPayintv(lcpol.getPayintv());//交费间隔
                    lcpFilter1.setInsuyear(lcpol.getInsuyear());//保险年龄年期
                    lcpFilter1.setPayendyearflag(lcpol.getPayendyearflag());//终交年龄年期标志
                    lcpFilter1.setInsuyearflag(lcpol.getInsuyearflag());//保险年龄年期标志

                    producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lcpFilter1)));

                    lcpFilter1.setPolno(null);
                    lcpFilter1.setContno(null);
                    lcpFilter1.setContplancode(null);
                    lcpFilter1.setMainpolno(null);
                    lcpFilter1.setRiskcode(null);
                    lcpFilter1.setPayendyear(null);
                    lcpFilter1.setAgentcode(null);
                    lcpFilter1.setPrem(null);

                    lcpFilter1.setPayintv(null);//交费间隔
                    lcpFilter1.setInsuyear(null);//保险年龄年期
                    lcpFilter1.setPayendyearflag(null);//终交年龄年期标志
                    lcpFilter1.setInsuyearflag(null);//保险年龄年期标志
                }
            }
        }
    }
}
