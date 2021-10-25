package com.pactera.yhl.f_policy.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.Lbinsured;
import com.pactera.yhl.entity.Lcaddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.pactera.yhl.util.Util.getHbaseResult;

/**
 * @author Sun Haitian
 * @Description o_lis_LCAddress add4 ,插入 ods.o_lis_LBInsured ib2 表数据, 取 险种被保人表(地址、电话)
 * @create 2021/10/14 15:36
 */
public class FP1Joinadd4_ib2_luc extends AbstractJoin<Lcaddress,Lbinsured>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> ib2Table;
    private AsyncTable<AdvancedScanResultConsumer> lucTable;
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;
    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            ib2Table = connection.getTable(TableName.valueOf("KL:LBINSURED_APPNTNO_ADDRESSNO"));
            lucTable = connection.getTable(TableName.valueOf("KL:LUCONT_CONTNO"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_INSUREDNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
//    left join o_lis_LCAddress add4 --险种被保人表(地址、电话)
//    on ib2.AppntNo = add4.CustomerNo and ib2.AddressNo = add4.AddressNo
//    ods.o_lis_LBInsured ib2

//    addressno, customerno
//    contno, insuredno

//    left join ods.o_lis_LBInsured ib2 --险种被保人表(地址、电话)
//    on luc.contno=ib2.contno  and luc.source_id='1' and  lup.insuredno=ib2.insuredno  --退保
    @Override
    public void asyncHandler(Lcaddress add4) throws Exception {
        //获取add4主键及关联字段
        String customerno = add4.getCustomerno();
        String addressno = add4.getAddressno();

        //插入宽表信息
        String add4Value = JSON.toJSONString(add4, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取ib2表信息
        Result ib2Result = getHbaseResult(customerno + addressno, this.ib2Table);
        if(!ib2Result.isEmpty()){
            for (Cell ib2ListCell : ib2Result.listCells()){
                String ib2Value = Bytes.toString(CellUtil.cloneValue(ib2ListCell));
                JSONObject ib2jsonObject = JSONObject.parseObject(ib2Value);
                //获取ib2主键及关联字段
                String ib2contno = ib2jsonObject.getString("contno");
                String ib2insuredno = ib2jsonObject.getString("insuredno");
                //通过关联条件获取luc表信息
                Result lucResult = getHbaseResult(ib2contno + "", this.lucTable);
                if(!lucResult.isEmpty()){
                    for (Cell lucListCell : lucResult.listCells()) {
                        String lucValue = Bytes.toString(CellUtil.cloneValue(lucListCell));
                        if("1".equals(JSON.parseObject(lucValue).getString("source_id"))){
                            //通过关联条件获取lup表信息
                            Result lupResult = getHbaseResult(ib2insuredno + "", this.lupTable);
                            if(!lupResult.isEmpty()){
                                for (Cell lupListCell : lupResult.listCells()){
                                    String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                                    //获取宽表主键
                                    String rowKey = JSON.parseObject(lupValue).getString("polno");
                                    Put put = new Put(Bytes.toBytes(rowKey));
                                    put.addColumn(cf, Bytes.toBytes("fp1_lcaddress_4" + addressno + customerno),
                                            Bytes.toBytes(add4Value));
                                    kuanbiao.put(put);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

}
