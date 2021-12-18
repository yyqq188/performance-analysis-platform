package com.pactera.yhl.apps.construction.util;

/**
 * @author: TSY
 * @create: 2021/12/7 0007 下午 18:15
 * @description:
 */
public class Utils {

    public static String int2StrIsNull(Integer s){
        return s == null ? "0" : s.toString();
    }

    public static Integer int2IntIsNull(Integer s){
        return s == null ? 0 : s;
    }

    public static String stringIsNull(String s){
        return "".equals(s) ? "" : s;
    }

    public static Integer str2IntIsNull(String s){
        return "".equals(s) ? 0 : Integer.valueOf(s);
    }


    public static Double str2DouIsNull(String s){
        return "".equals(s) ? 0.00 : Double.valueOf(s);
    }

}
