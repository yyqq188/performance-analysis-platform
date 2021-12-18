package com.pactera.yhl.apps.measure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ReadLine {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\1\\Desktop\\111.txt"));
        String line = null;
        String json = "";
        while ((line = br.readLine()) != null){
            json = json + line;
        }
        System.out.println(json);

    }
}
