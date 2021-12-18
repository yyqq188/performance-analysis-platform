package com.pactera.yhl.apps.measure.join.Lbopl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.SunUtils;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
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
 * @author Sun Haitian
 * @Description 处理主险
 * @create 2021/11/12 13:42
 */
public class Lpedoritem2Lbpol extends AbstractInsertKafka<Lbpol> {
    protected static int i = 1;
    public Lpedoritem2Lbpol(String topic){
        tableName = "KLMIDAPP:LBPOL_CONTNO";//HBase中间表名
        this.topic = topic; //"testyhlv3";  //目的topic
    }
    @Override
    public void handle(Lbpol value, Context context, HTable hTable) throws Exception {
        String contplancode = value.getContplancode();
        if (StringUtils.isBlank(contplancode)) {
            String contno = value.getContno();
            double payIntv = SunUtils.isDoubleNotNull(value.getPayintv());//交费间隔
            double payEndYear = SunUtils.isDoubleNotNull(value.getPayendyear());//终交年龄年期
            double insuYear = SunUtils.isDoubleNotNull(value.getInsuyear());//保险年龄年期
//            double payEndYearFlag = SunUtils.isDoubleNotNull(value.getPayendyearflag());//终交年龄年期标志
//            double insuYearFlag = SunUtils.isDoubleNotNull(value.getInsuyearflag());//保险年龄年期标志
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
//                    && payEndYearFlag > SunUtils.isDoubleNotNull(lbpol.getPayendyearflag()) && insuYearFlag > SunUtils.isDoubleNotNull(lbpol.getInsuyearflag())
                    if(payIntv < SunUtils.isDoubleNotNull(lbpol.getPayintv()) && payEndYear > SunUtils.isDoubleNotNull(lbpol.getPayendyear())
                            && insuYear > SunUtils.isDoubleNotNull(lbpol.getInsuyear()) ) {
                        //去主险的险种号做产品组合代码
                        value.setContplancode(lbpol.getRiskcode());
                    }
                }
            }else {
                value.setContplancode(value.getRiskcode());
            }
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(value)));
        }else {
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(value)));
        }

    }

}
