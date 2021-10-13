package com.pactera.yhl.util;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.lang.reflect.Field;

public class DemoMain {
    public static void main(String[] args) throws Exception {
        A a = new A();
        a.setName("aaa");
        a.setId("idaaa");

//        test(A.class);
        test2(a,A.class);


    }
    @Data
    static class A{
        private String name;
        private String id;

    }


    public static <T> void test2(Object o,Class<T> clazz) throws Exception{
        ObjectMapper objMapper = new ObjectMapper();
        A a = objMapper.convertValue(o, A.class);
        System.out.println(a.getId());


    }






    public static <T>  void test(Class<T> className) throws Exception {
        T t = className.newInstance();
        String name = t.getClass().getName();
        System.out.println(name);
        for(Field f:t.getClass().getDeclaredFields()){
            System.out.println(f.getName());
            f.setAccessible(true);
            System.out.println(f.get(t));
        }

//        ClassLoader classLoader = className.getClassLoader();
//        Class<?> aClass = classLoader.loadClass("com.pactera.yhl.util.DemoMain$A");
//        ((A) aClass).getField("id");

    }
}
