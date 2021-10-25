package com.pactera.yhl.f_policy.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KLEntity;
import com.pactera.yhl.entity.Lcappnt;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zy
 * @version 1.0
 * @description:
 * @date 2021/10/12 15:25
 */
public class Lcappnt_AddressNoSink  extends AbstractInsertHbase<KLEntity>{

    public Lcappnt_AddressNoSink(){
        tableName = "KL:LCAPPNT_ADDRESSNO";//HBase中间表名
        rowkeys = new String[]{"addressno"};//HBase rowkeys
        //POJO主键---HBase列名
        columnNames = new String[]{"contno"};
        columnTableName = "lcappnt";//表名
    }

    @Override
    public void handle(KLEntity klEntity, Context context, HTable table) throws Exception {
        Lcappnt value = (Lcappnt) klEntity;
        StringBuilder rowkeySb = new StringBuilder();
        StringBuilder columnSb = new StringBuilder();
        columnSb.append(columnTableName);
        try {
            Map<String, Object> map = new HashMap<String, Object>();

            for (String rowkey : rowkeys) {
                map.put("value", value);
                map.put("rowkey", rowkey);
                String expression = "value.getStateflag(rowkey)";
                Object value1 = Util.convertToCode(expression, map);
                rowkeySb.append(value1);
            }

            if (columnNames.length > 0) {
                for (String rowkey : columnNames) {
                    map.put("value", value);
                    map.put("rowkey", rowkey);
                    String expression = "value.getStateflag(rowkey)";
                    Object value1 = Util.convertToCode(expression, map);
                    columnSb.append(value1);
                }
            }
//            Put put = new Put(Bytes.toBytes(value.getChdrcoy() + value.getChdrnum()));
            Put put = new Put(Bytes.toBytes(rowkeySb.toString()));
            String valueJson = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
            put.addColumn(cf, Bytes.toBytes(columnSb.toString()), Bytes.toBytes(valueJson));
            table.put(put);

        } catch (Exception e) {

        }
        System.out.println("tableName is " + tableName);
    }
}
