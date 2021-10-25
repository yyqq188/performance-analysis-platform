package com.pactera.yhl.f_agent.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KLEntity;
import com.pactera.yhl.entity.T01teaminfo;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: TSY
 * @create: 2021/10/19 0019 下午 18:32
 * @description:
 */
public class OXTT_Team_IdSink extends AbstractInsertHbase<T01teaminfo> {
    public OXTT_Team_IdSink(){
        tableName = "KLMID:O_XG_T01TEAMINFO_TEAM_ID";//HBase中间表名
        rowkeys = new String[]{"team_id"};//HBase rowkeys
        //POJO主键---HBase列名
        columnNames = new String[]{"channel_id"};
        columnTableName = "o_xg_t01teaminfo";//表名
    }
    @Override
    public void handle(T01teaminfo value, Context context, HTable table) throws Exception {
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
