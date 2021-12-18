package com.pactera.yhl.apps.develop.demo.demo1;

import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity01;
import com.pactera.yhl.util.Util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class Demo1 {
    public static void main(String[] args) throws Exception, InstantiationException, NoSuchMethodException {
//        Enty enty = new Enty();
//        enty.setAddress("address");
//        enty.setId("id");
//        enty.setName("name");
//        Field[] declaredFields = enty.getClass().getDeclaredFields();
//        for(Field f:declaredFields){
//            System.out.println(f.getName());
//
//            System.out.println("---");
//            System.out.println(f.get(enty));
//
//        }
        //给反射的类型赋值
//        Class<?> clazz = PremiumsKafkaEntity01.class;
//        Object o = clazz.newInstance();
//        String methodName = "set" + Util.LargerFirstChar("workarea");
//        Method method = clazz.getDeclaredMethod(methodName, String.class);
//        method.invoke(o,"aaaworkaera");
//        System.out.println(o);
//
//        //Java通过Class类型将Object转换为相应类型
//        Object o1 = clazz.newInstance();
//        PremiumsKafkaEntity01.class.cast(o1);



//        PremiumsKafkaEntity01 premiumsKafkaEntity01 = new PremiumsKafkaEntity01();
//        premiumsKafkaEntity01.setWorkarea(null);
//        premiumsKafkaEntity01.setPrem("prem");
//        premiumsKafkaEntity01.setManagecom("managecom");
//        premiumsKafkaEntity01.setAgentcom("agentcom");
//        premiumsKafkaEntity01.setChannel_id("channel_id");
//
//        Field field = premiumsKafkaEntity01.getClass().getField("workarea");
//        Object o = field.get(premiumsKafkaEntity01);
//        if(Objects.isNull(o)){
//            System.out.println("空");
//        }else{
//            System.out.println("非空");
//        }
//        System.out.println(o);

//        //将hbase的值赋值到kafka实体类中
//        String methodName = "set"+Util.LargerFirstChar(fieldName);
//        Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
//        method.invoke(kafkaClazzObj,v);


        String a = "abc";
        System.out.println(a.substring(1,a.length()));

    }

}
