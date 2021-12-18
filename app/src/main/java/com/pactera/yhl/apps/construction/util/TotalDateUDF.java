package com.pactera.yhl.apps.construction.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author: TSY
 * @create: 2021/11/29 0029 下午 14:04
 * @description:  获取日期
 */
public class TotalDateUDF {

    static SimpleDateFormat formatTotal = new SimpleDateFormat("yyyy-MM-dd");
    static SimpleDateFormat format2Total = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //获取当天日期年月日时分秒
    public synchronized static String getCurrentDateTotal(){
        return format2Total.format(System.currentTimeMillis());
    }

    //获取当天日期年月日
    public synchronized static String getCurrentDayTotal(){
        return formatTotal.format(System.currentTimeMillis());
    }

    //获取当前日期前一天
    public synchronized static String getbeforeDayTotal(){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,   -1);
        return formatTotal.format(cal.getTime());//第一天
    }

    //获取当月的第一天
    public synchronized static String getFirstDayOfMonthTotal(){
        Calendar first = Calendar.getInstance();
        first.add(Calendar.MONTH, 0);
        first.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
        return formatTotal.format(first.getTime());//第一天
    }

    public synchronized static String getEndDayOfMonthTotal(){
        Calendar last = Calendar.getInstance();
        last.set(Calendar.DAY_OF_MONTH, last.getActualMaximum(Calendar.DAY_OF_MONTH));
        return formatTotal.format(last.getTime());//最后一天
    }
}
