package com.pactera.yhl;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import java.util.Map;

public class Util {
    //字符串转可执行程序
    public static Object convertToCode(String jexlExp, Map<String,Object> map){
        JexlEngine jexl=new JexlEngine();
        Expression e = jexl.createExpression(jexlExp);
        JexlContext jc = new MapContext();
        for(String key:map.keySet()){
            jc.set(key, map.get(key));
        }
        if(null==e.evaluate(jc)){
            return "";
        }
        return e.evaluate(jc);
    }

    //首字母大写
    public static String LargerFirstChar(String str) {
        char[] chars = str.toCharArray();
        chars[0] -= 32;
        if(chars[0]>97){

        }
        return String.valueOf(chars);
    }
}
