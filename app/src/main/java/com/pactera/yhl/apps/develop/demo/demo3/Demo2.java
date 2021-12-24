package com.pactera.yhl.apps.develop.demo.demo3;

public class Demo2 {
    public static void main(String[] args) {
        String aa = "aaa";
        String[] split = aa.split(",");
        for(String s:split){
            System.out.println(s);
        }
    }
}
