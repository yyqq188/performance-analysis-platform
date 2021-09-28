package map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import entity.KLEntity;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * @author SUN KI
 * @time 2021/9/27 19:52
 * @Desc
 */
public class MapFuncTableAnychatcont extends RichMapFunction<String, KLEntity> {
    private static final Logger logger = LoggerFactory.getLogger(MapFuncTableAnychatcont.class);

    @Override
    public KLEntity map(String s) throws Exception {
        JSONObject jsonObject = JSON.parseObject(s);
        if(!jsonObject.containsKey("message")) return null;

        JSONObject message = jsonObject.getJSONObject("message");
        if(!message.containsKey("meta_data")) return null;
        JSONArray meta_data = message.getJSONArray("meta_data");
        String tableName = meta_data.getJSONObject(1).getJSONObject("value").getString("string");

        logger.info("tableName is {}",tableName);
        KLEntity entity = EntityStrategy.getEntity(tableName);
        Field[] declaredFields = entity.getClass().getDeclaredFields();
        return null;
    }
}
