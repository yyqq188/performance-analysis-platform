package com.pactera.yhl.f_agent.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.T02salesinfo_k;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: TSY
 * @create: 2021/10/20 0020 下午 16:11
 * @description:
 */
public class OXTO_Team_IdSink extends AbstractInsertHbase<T02salesinfo_k> {
    public OXTO_Team_IdSink(){
        tableName = "KLMID:O_XG_T02SALESINFO_K_TEAM_ID";//HBase中间表名
        rowkeys = new String[]{"team_id"};//HBase rowkeys
        //POJO主键---HBase列名
        columnNames = new String[]{"sales_id"};
        columnTableName = "o_xg_t02salesinfo_k";//表名
    }
    @Override
    public void handle(T02salesinfo_k value, Context context, HTable table) throws Exception {
        StringBuilder rowkeySb = new StringBuilder();
        StringBuilder columnSb = new StringBuilder();
        columnSb.append(columnTableName);
        try{
            Map<String,Object> map=new HashMap<String,Object>();

            for(String rowkey:rowkeys){
                map.put("value",value);
                String expression = "value.get"+Util.LargerFirstChar(rowkey)+"()";
                Object value1 = Util.convertToCode(expression,map);
                rowkeySb.append(value1);
            }

            if(columnNames.length > 0 ) {
                for(String rowkey:columnNames){
                    map.put("value",value);
                    String expression = "value.get"+Util.LargerFirstChar(rowkey)+"()";
                    Object value1 = Util.convertToCode(expression,map);
                    columnSb.append(value1);
                }
            }
//            Put put = new Put(Bytes.toBytes(value.getChdrcoy() + value.getChdrnum()));
            Put put = new Put(Bytes.toBytes(rowkeySb.toString()));
            String valueJson = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
            put.addColumn(cf, Bytes.toBytes(columnSb.toString()), Bytes.toBytes(valueJson));
            table.put(put);

        }catch (Exception e){

        }
        System.out.println("tableName is "+tableName);
    }
}
