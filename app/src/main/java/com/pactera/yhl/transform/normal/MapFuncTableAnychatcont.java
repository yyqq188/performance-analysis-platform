package com.pactera.yhl.transform.normal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.transform.normal.entity.KLEntity;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Field;

/**
 * @author yhl
 * @time 2021/9/27 19:52
 * @Desc
 */
public class MapFuncTableAnychatcont extends RichMapFunction<String, KLEntity> {
    private static final Logger logger = LoggerFactory.getLogger(MapFuncTableAnychatcont.class);

    @Override
    public KLEntity map(String source) throws Exception {
        JSONObject jsonObject = JSON.parseObject(source);
        if(!jsonObject.containsKey("message")) return null;

        String message1 = jsonObject.getString("message")
                .replace("\\", "\\\\");
        JSONObject message = JSON.parseObject(message1);

        if(!message.containsKey("meta_data")) return null;
        JSONArray meta_data = message.getJSONArray("meta_data");
        String tableName = meta_data.getJSONObject(1)
                .getJSONObject("value")
                .getString("string");

        JSONArray columns = message
                .getJSONObject("columns")
                .getJSONArray("array");


        KLEntity entity = EntityStrategy.getEntity(tableName);
        if(entity == null) return null;
        for(Object o:columns){
            JSONObject columObj = JSON.parseObject(o.toString());
            if(columObj.containsKey("value") && columObj.containsKey("name")){
                String value = columObj.getJSONObject("value").getString("string");
                String name = columObj.getJSONObject("name").getString("string").toLowerCase();
                try{
                    Field field = entity.getClass().getDeclaredField(name);
                    field.setAccessible(true);
                    field.set(entity,value);
                }catch (Exception e){
                    continue;
                }

            }
        }
        return entity;
    }

}
