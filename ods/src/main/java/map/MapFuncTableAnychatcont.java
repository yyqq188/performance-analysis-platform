package map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import entity.anychatcont;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

/**
 * @author SUN KI
 * @time 2021/9/27 19:52
 * @Desc
 */
public class MapFuncTableAnychatcont extends RichMapFunction<String, anychatcont> {

    /*static Map<String, String> metaMap = new HashMap<>();
    static Map<String, Tuple2<String, String>> clmsMap = new HashMap<>();*/

    @Override
    public anychatcont map(String json) throws Exception {
        anychatcont anychatcont = null;
        Map<String, String> metaMap = null;
        Map<String, Tuple2<String, String>> clmsMap = null;
        /*metaMap.clear();
        clmsMap.clear();*/
        JSONArray metaArr = JSON.parseObject(json).getJSONArray("meta_data");
        JSONArray columnsArr = JSON.parseObject(json).getJSONObject("columns").getJSONArray("array");
//        Map<String, String> metaMap = new HashMap<>();


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
        String table = metaMap.get("INFA_TABLE_NAME");
        String tableName = table.split("_")[1].toLowerCase();


        if (tableName.equals(anychatcont.class.getName())) {
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
            anychatcont.setBusinessno(clmsMap.get("businessno").f0);
            anychatcont.setDrsflag(clmsMap.get("drsflag").f0);
            anychatcont.setBusinessno(clmsMap.get("checkdate").f0);
            anychatcont.setBusinessno(clmsMap.get("ischeck").f0);
            anychatcont.setBusinessno(clmsMap.get("checkresult").f0);
            anychatcont.setBusinessno(clmsMap.get("fcd").f0);
            anychatcont.setBusinessno(clmsMap.get("flag1").f0);
            anychatcont.setBusinessno(clmsMap.get("flag2").f0);
            anychatcont.setBusinessno(clmsMap.get("flag3").f0);
            anychatcont.setBusinessno(clmsMap.get("flag4").f0);
            anychatcont.setBusinessno(clmsMap.get("operator").f0);
            anychatcont.setBusinessno(clmsMap.get("makedate").f0);
            anychatcont.setBusinessno(clmsMap.get("maketime").f0);
            anychatcont.setBusinessno(clmsMap.get("modifydate").f0);
            anychatcont.setBusinessno(clmsMap.get("modifytime").f0);
            anychatcont.setBusinessno(clmsMap.get("etl_dt").f0);
            anychatcont.setBusinessno(clmsMap.get("etl_tm").f0);
            anychatcont.setBusinessno(clmsMap.get("etl_fg").f0);
            anychatcont.setBusinessno(clmsMap.get("op_ts").f0);
            anychatcont.setBusinessno(clmsMap.get("current_ts").f0);
            anychatcont.setBusinessno(clmsMap.get("load_date").f0);
        }

        return anychatcont;
    }
}
