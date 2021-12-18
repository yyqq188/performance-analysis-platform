package com.pactera.yhl.apps.measure.join.Lcpol;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.SunUtils;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Sun Haitian
 * @Description 处理主险
 * @create 2021/11/17 15:46
 */
public class Lcpol2Lcpol extends AbstractInsertKafka<Lcpol> {
    public static int i = 1;
    public Lcpol2Lcpol(String topic){
        tableName = "KLMIDAPP:LCPOL_CONTNO";//HBase中间表名
        this.topic = topic;
    }
    @Override
    public void handle(Lcpol value, Context context, HTable hTable) throws Exception {
        String salechnl = value.getSalechnl();//销售渠道
        //仅限 银保渠道 , 剔除 投保单/个单/续保
        if(("04".equalsIgnoreCase(salechnl) || "13".equalsIgnoreCase(salechnl))
                && !"0".equalsIgnoreCase(value.getStateflag()) && "0".equalsIgnoreCase(value.getRenewcount())
                && "1".equalsIgnoreCase(value.getConttype())){
            String contplancode = value.getContplancode();
            //判断产品组合代码是否为空,为空取主险的险种号
            if (StringUtils.isBlank(contplancode)) {
                String contno = value.getContno();
                double payIntv = SunUtils.isDoubleNotNull(value.getPayintv());//交费间隔
                double payEndYear = SunUtils.isDoubleNotNull(value.getPayendyear());//终交年龄年期
                double insuYear = SunUtils.isDoubleNotNull(value.getInsuyear());//保险年龄年期
                //double payEndYearFlag = SunUtils.isDoubleNotNull(value.getPayendyearflag());//终交年龄年期标志
                //double insuYearFlag = SunUtils.isDoubleNotNull(value.getInsuyearflag());//保险年龄年期标志
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
                        // && payEndYearFlag > SunUtils.isDoubleNotNull(lcpol.getPayendyearflag())
                        // && insuYearFlag > SunUtils.isDoubleNotNull(lcpol.getInsuyearflag())
                        if(payIntv < SunUtils.isDoubleNotNull(lcpol.getPayintv()) && payEndYear > SunUtils.isDoubleNotNull(lcpol.getPayendyear())
                                && insuYear > SunUtils.isDoubleNotNull(lcpol.getInsuyear()) ){
                            //取主险的险种号做产品组合代码
                            value.setContplancode(lcpol.getRiskcode());
                        }
                    }
                }else {
                    value.setContplancode(value.getRiskcode());
                }
            }
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(value)));
            System.out.println(i++);
        }

    }

}
