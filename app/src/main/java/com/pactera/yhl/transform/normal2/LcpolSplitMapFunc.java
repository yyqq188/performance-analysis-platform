package com.pactera.yhl.transform.normal2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.entity.source.Lcpol;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class LcpolSplitMapFunc extends RichMapFunction<String, Lcpol> {
    static Map<String, String> metaMap = new HashMap<>();
    static Map<String, Tuple2<String, String>> clmsMap = new HashMap<>();
    @Override
    public Lcpol map(String json) throws Exception {
        Lcpol lcpol = new Lcpol();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTs = sdf.format(System.currentTimeMillis());
        metaMap.clear();
        clmsMap.clear();
        String tableName;
        String type;

        JSONObject jsonObject = JSON.parseObject(json);
        String message = jsonObject.getString("message");
        if(StringUtils.isNotBlank(message)){
            JSONArray metaArr = JSON.parseObject(message).getJSONArray("meta_data");
            JSONArray columnsArr = JSON.parseObject(message).getJSONObject("columns").getJSONArray("array");

            if (!metaArr.isEmpty()) {
                for (Object obj : metaArr) {//遍历metaArr中的每个值，值也是json，需解析
                    JSONObject js = JSON.parseObject(String.valueOf(obj));//得到meta中的一个值，并解析成json

                    JSONObject name = js.getJSONObject("name");
                    String key = name.getString("string");//得到name属性对应的值，即fieldName
                    //开始解析出字段值，需要注意字段值可能为空，需要做判断
                    JSONObject val = js.getJSONObject("value");

                    String value;
                    if (val != null) {
                        value = val.getString("string");
                    } else {
                        value = "";
                    }

                    metaMap.put(key, value);
                }
            }

            if (!columnsArr.isEmpty()) {
                for (Object obj : columnsArr) {
                    JSONObject js = JSON.parseObject(String.valueOf(obj));
                    String fieldName = js.getJSONObject("name").getString("string").toLowerCase();
                    JSONObject val = js.getJSONObject("value");
                    JSONObject beforeVal = js.getJSONObject("beforeImage");

                    String value;  //更新后的值
                    String beforeValue;//更新前的值

                    if (val != null) {
                        value = val.getString("string");
                    } else {
                        value = " ";
                    }

                    if (beforeVal != null) {
                        beforeValue = beforeVal.getString("string");
                    } else {
                        beforeValue = " ";
                    }
                    clmsMap.put(fieldName, new Tuple2<>(value, beforeValue));
                }
            }
            String table = metaMap.get("INFA_TABLE_NAME");
            if(StringUtils.isNotBlank(table)){
                tableName = table.split("_")[1].toLowerCase();
//        System.out.println("tableName = " + tableName);
                if (tableName.contains("lb")){
                    System.out.println("tableName = " + tableName);
                }
                type = metaMap.get("INFA_OP_TYPE");

                //仅 lcpol 且 insert 操作的数计算
                if("lcpol".equalsIgnoreCase(tableName) &&
                        ("insert_event".equalsIgnoreCase(type) || "update_event".equalsIgnoreCase(type))){
                    Field[] fields = lcpol.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        String fieldName = field.getName();
                        field.setAccessible(true);
                        //将签单日期转为yyyy-MM-dd
                        if("signdate".equalsIgnoreCase(fieldName)){
                            String signdate = clmsMap.getOrDefault("signdate", new Tuple2<>("", "")).f0;
                            if (" ".equals(signdate) || signdate == null) {
                                field.set(lcpol,signdate);
                            } else {
                                field.set(lcpol,signdate.substring(0, 4) + "-" + signdate.substring(4, 6) + "-" + signdate.substring(6, 8));
                            }
                        }else {
                            field.set(lcpol,clmsMap.getOrDefault(fieldName,new Tuple2<>("", "")).f0);
                        }
                    }
                    lcpol.setCurrent_ts(currentTs);
                    return lcpol;
                }
            }
        }

        //返回值
        return null;
    }
}
