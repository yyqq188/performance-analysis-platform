package com.pactera.yhl.apps.warning.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author SUN KI
 * @time 2021/11/16 10:08
 * @Desc
 */
public class DateCal {
    public static void main(String[] args) {
//        getCurrentQuarterStartTime();
//        transProbationDate("2021年12月31日");
        long l = System.currentTimeMillis();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = formatter.format(l);
        System.out.println(currentTime);
    }
    public static String getCurrentQuarterStartTime() {
        Calendar c = Calendar.getInstance();
        int currentMonth = c.get(Calendar.MONTH) + 1;
        SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");
        String s = null;
        try {
            if (currentMonth <= 3)
                c.set(Calendar.MONTH, 0);
            else if (currentMonth <= 6)
                c.set(Calendar.MONTH, 3);
            else if (currentMonth <= 9)
                c.set(Calendar.MONTH, 6);
            else if (currentMonth <= 12)
                c.set(Calendar.MONTH, 9);
            c.set(Calendar.DATE, 15);
            c.set(Calendar.MONTH, c.get(Calendar.MONTH)+1);
            s = shortSdf.format(c.getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(s);
        return s;
    }

    public static String transProbationDate(String str) {
        String monthStr;
        String yearStr;
        String dayStr;
        String probationDate;
        int startIndex = str.indexOf("年");
        int endIndex = str.indexOf("月");
        monthStr = str.substring(startIndex + 1, endIndex);
        if (monthStr.length() == 1){
            monthStr = "0" + monthStr;
        }
        yearStr = str.substring(0,startIndex);
        dayStr = str.substring(endIndex + 1, str.length() - 1);
        if (dayStr.length() == 1){
            dayStr = "0" + dayStr;
        }
        probationDate = yearStr + "-" + monthStr + "-" + dayStr;
        System.out.println(probationDate);
        return probationDate;
    }
}
