package com.pactera.yhl.apps.measure;

import java.lang.reflect.Field;
import java.lang.reflect.Method;


public class SunTest {
    public static void main(String[] args) throws Exception {
        A a = new A();
        a.setName("Sun");
        a.setAge("23");
        a.setSex("F");
        B b = new B();
        Field[] fields = b.getClass().getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            Object value = getGetMethod(a, name);
            field.setAccessible(true);
            field.set(b,value);
        }
        System.out.println(b.toString());
    }
    public static class A{
        public A() {
        }

        public A(String name, String age, String sex) {
            this.name = name;
            this.age = age;
            this.sex = sex;
        }

        public String name;
        public String age;
        public String sex;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }
    }
    public static class B{
        public B() {
        }

        public B(String name, String age) {
            this.name = name;
            this.age = age;
        }

        public String name;
        public String age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "B{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
    public static Object getGetMethod(Object ob , String name)throws Exception{
        Method[] m = ob.getClass().getMethods();
        for(int i = 0;i < m.length;i++){
            if(("get"+name).toLowerCase().equals(m[i].getName().toLowerCase())){
                return m[i].invoke(ob);
            }
        }
        return null;
    }

}
