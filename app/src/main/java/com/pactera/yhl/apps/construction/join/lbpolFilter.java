package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.construction.entity.LupFilter;
import com.pactera.yhl.apps.construction.util.DateUDF;
import com.pactera.yhl.entity.source.Lbpol;
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
 * @create: 2021/11/19 0019 上午 10:25
 * @description:
 */
public class lbpolFilter extends AbstractInsertKafka<Lbpol> {

    public lbpolFilter(String topic) {
        tableName = "KLMIDAPP:LPEDORITEM_EDORNO_CONTNO";//HBase中间表名
        this.topic = topic;//目的topic
    }
    private static LupFilter lbpFilter1 = new LupFilter();

    @Override
    public void handle(Lbpol lbpol, Context context, HTable hTable) throws Exception {
        String salechnl = lbpol.getSalechnl();
        String renewcount = lbpol.getRenewcount();
        String conttype = lbpol.getConttype();
        //todo 保单卡银保渠道
        if ("04".equals(salechnl) || "13".equals(salechnl)) {
            //todo lup.RenewCount=0 --剔除续保and lup.ContType = '1' --1为个单
            if ("0".equals(renewcount) && "1".equals(conttype)) {
                String edorno = lbpol.getEdorno();
                //todo edrno != 'XB%'
                if (!"XB".equalsIgnoreCase(edorno.substring(0, 2))) {

                    String contno = lbpol.getContno();
                    //System.out.println("edorno + contno :::"+edorno + contno);
                    //todo 过滤lbp.edorno like 'YBT%'为当日撤单
                    if ("YBT".equalsIgnoreCase(edorno.substring(0, 3))) {
                        lbpFilter1.setPolno(lbpol.getPolno());
                        lbpFilter1.setEdorno(lbpol.getEdorno());
                        lbpFilter1.setContno(lbpol.getContno());
                        lbpFilter1.setContplancode(lbpol.getContplancode());
                        lbpFilter1.setMainpolno(lbpol.getMainpolno());
                        lbpFilter1.setRiskcode(lbpol.getRiskcode());
                        lbpFilter1.setPayendyear(lbpol.getPayendyear());
                        lbpFilter1.setSigndate("");
                        lbpFilter1.setAgentcode(lbpol.getAgentcode());
                        lbpFilter1.setPrem(lbpol.getPrem());
                        lbpFilter1.setMark("lbpol");

                        lbpFilter1.setPayintv(lbpol.getPayintv());//交费间隔
                        lbpFilter1.setInsuyear(lbpol.getInsuyear());//保险年龄年期
                        lbpFilter1.setPayendyearflag(lbpol.getPayendyearflag());//终交年龄年期标志
                        lbpFilter1.setInsuyearflag(lbpol.getInsuyearflag());//保险年龄年期标志
                    } else {
                        //获取lpedoritem中间表数据
                        Result resultLpe = Util.getHbaseResultSync(edorno + contno + "", hTable);
                        if (!resultLpe.isEmpty()) {
                            for (Cell cell : resultLpe.listCells()) {
                                String valueJson = Bytes.toString(CellUtil.cloneValue(cell));
                                Lpedoritem lpe = JSON.parseObject(valueJson, Lpedoritem.class);
                                //todo lp.EDORTYPE='WT' and  过滤lp.EDORSTATE='0'--犹豫期退保  and 犹退Modifydate
                                if ("WT".equalsIgnoreCase(lpe.getEdortype()) && "0".equals(lpe.getEdorstate()) && DateUDF.getCurrentDay().equals(lpe.getModifydate())) {
                                    lbpFilter1.setPolno(lbpol.getPolno());
                                    lbpFilter1.setEdorno(lbpol.getEdorno());
                                    lbpFilter1.setContno(lbpol.getContno());
                                    lbpFilter1.setContplancode(lbpol.getContplancode());
                                    lbpFilter1.setMainpolno(lbpol.getMainpolno());
                                    lbpFilter1.setRiskcode(lbpol.getRiskcode());
                                    lbpFilter1.setPayendyear(lbpol.getPayendyear());
                                    lbpFilter1.setSigndate("");
                                    lbpFilter1.setAgentcode(lbpol.getAgentcode());
                                    lbpFilter1.setPrem(lbpol.getPrem());
                                    lbpFilter1.setMark("lbpol");

                                    lbpFilter1.setPayintv(lbpol.getPayintv());//交费间隔
                                    lbpFilter1.setInsuyear(lbpol.getInsuyear());//保险年龄年期
                                    lbpFilter1.setPayendyearflag(lbpol.getPayendyearflag());//终交年龄年期标志
                                    lbpFilter1.setInsuyearflag(lbpol.getInsuyearflag());//保险年龄年期标志
                                }
                            }
                        }
                    }

                    producer.send(new ProducerRecord<>(topic, JSON.toJSONString(lbpFilter1)));

                    lbpFilter1.setPolno(null);
                    lbpFilter1.setEdorno(null);
                    lbpFilter1.setContno(null);
                    lbpFilter1.setContplancode(null);
                    lbpFilter1.setMainpolno(null);
                    lbpFilter1.setRiskcode(null);
                    lbpFilter1.setPayendyear(null);
                    lbpFilter1.setSigndate(null);
                    lbpFilter1.setAgentcode(null);
                    lbpFilter1.setPrem(null);

                    lbpFilter1.setPayintv(null);//交费间隔
                    lbpFilter1.setInsuyear(null);//保险年龄年期
                    lbpFilter1.setPayendyearflag(null);//终交年龄年期标志
                    lbpFilter1.setInsuyearflag(null);//保险年龄年期标志
                }
            }
        }
    }
}
