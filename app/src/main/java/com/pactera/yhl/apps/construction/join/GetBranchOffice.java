package com.pactera.yhl.apps.construction.join;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.construction.entity.ConstructionPOJO;
import com.pactera.yhl.apps.construction.entity.InductionManpower;
import com.pactera.yhl.apps.construction.entity.PremiumsPOJO;
import com.pactera.yhl.apps.construction.util.DateUDF;
import com.pactera.yhl.entity.source.T01branchinfo;
import com.pactera.yhl.entity.source.T02salesinfok;
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
 * @create: 2021/11/15 0015 下午 14:18
 * @description: 关联获取分公司代码
 */
public class GetBranchOffice extends AbstractInsertKafka<T02salesinfok> {

    public GetBranchOffice(String topic) {
        tableName = "KLMIDAPP:T01BRANCHINFO_BRANCH_ID";//HBase中间表名
        this.topic = topic;//目的topic
    }
    //private static InductionManpower im = new InductionManpower();
    private static PremiumsPOJO ppj = new PremiumsPOJO();
    private static T01branchinfo t01bGBO = null;
    @Override
    public void handle(T02salesinfok t02, Context context, HTable hTable) throws Exception {
        System.out.println(DateUDF.getCurrentDay());
        System.out.println(t02.getProbation_date().substring(0, 10));
        //todo 当日入职
        if (DateUDF.getCurrentDay().equals(t02.getProbation_date().substring(0, 10)) && "1".equals(t02.getStat()) || DateUDF.getCurrentDay().equals(t02.getDismiss_date().substring(0, 10)) && "2".equals(t02.getStat())) {
            //todo 银保渠道
            if ("08".equals(t02.getChannel_id())) {
                //todo 年份为2021
                if ("2021".equals(t02.getVersion_id())) {
                    //todo 关联字段
                    String branch_id = t02.getBranch_id();
                    //System.out.println("branch_id:"+branch_id);
                    //todo 获取T01中间表数据
                    t01bGBO = getBranch_id(branch_id, hTable);
                    if (t01bGBO != null) {
                        //todo 递归获取分公司
                        while (!"2".equals(t01bGBO.getClass_id())) {
                            String branch_id_parent = t01bGBO.getBranch_id_parent();
                            t01bGBO = getBranch_id(branch_id_parent, hTable);
                        }
                        String branch_id2 = t01bGBO.getBranch_id();
                        System.out.println("分公司代码：：：" + branch_id2);
                        String branch_name = t01bGBO.getBranch_name();
                        String rank = t02.getRank();
                        String stat = t02.getStat();
                        String sales_id = t02.getSales_id();


                        ppj.setKey_id(DateUDF.getCurrentDay() + "#86" + branch_id2.substring(0, 2));//主键
                        ppj.setBranch_name(branch_name);//公司名称
                        ppj.setRank(rank);//职级
                        ppj.setStat(stat);//是否签约在职
                        ppj.setSales_id(sales_id);//人员ID
                        ppj.setPrem("0");//保费
                        ppj.setProbation_date("-");//签约日期
                        ppj.setSigndate("-");//签单日期
                        ppj.setMark("-");//标志
                        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(ppj)));
                        ppj.setKey_id(null);
                        ppj.setSales_id(null);
                        ppj.setBranch_name(null);//公司名称
                        ppj.setRank(null);
                        ppj.setStat(null);
                    }
                }
            }
        }
    }

    //传入branch_id获取T01branchinfo对象·
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