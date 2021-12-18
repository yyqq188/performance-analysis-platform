package com.pactera.yhl.apps.construction.util;

/**
 * @author: TSY
 * @create: 2021/12/7 0007 下午 18:15
 * @description:
 */
public class TotalUtils {

    public static String int2StrIsNullTotal(Integer s){
        return s == null ? "0" : s.toString();
    }

    public static Integer int2IntIsNullTotal(Integer s){
        return s == null ? 0 : s;
    }

    public static String stringIsNullTotal(String s){
        return "".equals(s) ? "" : s;
    }

    public static Integer str2IntIsNullTotal(String s){
        return "".equals(s) ? 0 : Integer.valueOf(s);
    }


    public static Double str2DouIsNullTotal(String s){
        return "".equals(s) ? 0.00 : Double.valueOf(s);
    }

}
