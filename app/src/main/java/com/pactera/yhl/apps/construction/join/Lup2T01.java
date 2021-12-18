package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.construction.entity.EffectiveManpower;
import com.pactera.yhl.apps.construction.entity.LupToT02;
import com.pactera.yhl.apps.construction.entity.PremiumsPOJO;
import com.pactera.yhl.entity.source.T01branchinfo;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;

/**
 * @author: TSY
 * @create: 2021/11/17 0017 下午 13:14
 * @description:
 */
public class Lup2T01 extends AbstractInsertKafka<LupToT02> {

    public Lup2T01(String topic) {
        tableName = "KLMIDAPP:T01BRANCHINFO_BRANCH_ID";//HBase中间表名
        this.topic = topic; //"testyhlv3";  //目的topic
    }
    String dt = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis());
    //private static EffectiveManpower em = new EffectiveManpower();
    private static PremiumsPOJO pp = new PremiumsPOJO();
    private static T01branchinfo t01LT = null;
    @Override
    public void handle(LupToT02 lutt, Context context, HTable hTable) throws Exception {
        String branch_id = lutt.getBranch_id();
        //todo 获取T01中间表数据
        t01LT = getBranch_id(branch_id, hTable);
        if (t01LT != null) {
            //todo 递归获取分公司
            while (!"2".equals(t01LT.getClass_id())) {
                String branch_id_parent = t01LT.getBranch_id_parent();
                t01LT = getBranch_id(branch_id_parent, hTable);
            }

            //todo 如果是宁波支公司要把保费算到浙江分公司上
            String branch_id2 = "920000".equals(t01LT.getBranch_id()) ? "330000": t01LT.getBranch_id();
            String branch_name = null;
            if("330000".equals(branch_id2)) {
                branch_name = "浙江分公司";
            }else {
                branch_name = t01LT.getBranch_name();
            }

            pp.setKey_id(dt + "#86" +branch_id2.substring(0, 2));//日期+分公司代码
            pp.setRank("-");
            pp.setBranch_name(branch_name);//公司名称
            pp.setSales_id(lutt.getSales_id());//人员ID
            pp.setPrem(lutt.getPrem());//保费
            pp.setStat("-");//状态：是否在职
            pp.setProbation_date(lutt.getProbation_date());//签约日期
            pp.setSigndate(lutt.getSigndate());//签单日期
            pp.setMark(lutt.getMark());
            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(pp)));
            pp.setKey_id(null);
            pp.setBranch_name(null);//公司名称
            pp.setSales_id(null);//人员ID
            pp.setPrem(null);
            //em.setStat(null);//状态：是否在职
            pp.setProbation_date(null);//签约日期
            pp.setSigndate(null);//签单日期
            pp.setMark(null);
        }
    }

    //传入branch_id获取T01branchinfo对象
    public T01branchinfo getBranch_id(String branch_id, HTable hTable) throws Exception {
        Result resultT01 = Util.getHbaseResultSync(branch_id + "", hTable);
        if (!resultT01.isEmpty()) {
            for (Cell cell : resultT01.listCells()) {
                String valueJson = Bytes.toString(CellUtil.cloneValue(cell));
                return JSON.parseObject(valueJson, T01branchinfo.class);
            }
        }
        return null;
    }
}