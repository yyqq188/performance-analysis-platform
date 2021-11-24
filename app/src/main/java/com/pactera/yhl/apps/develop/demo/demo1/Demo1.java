package com.pactera.yhl.apps.develop.demo.demo1;

import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity01;
import com.pactera.yhl.util.Util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

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
        Class<?> clazz = PremiumsKafkaEntity01.class;
        Object o = clazz.newInstance();
        String methodName = "set" + Util.LargerFirstChar("workarea");
        Method method = clazz.getDeclaredMethod(methodName, String.class);
        method.invoke(o,"aaaworkaera");
        System.out.println(o);

        //Java通过Class类型将Object转换为相应类型
        Object o1 = clazz.newInstance();
        PremiumsKafkaEntity01.class.cast(o1);



    }

}
