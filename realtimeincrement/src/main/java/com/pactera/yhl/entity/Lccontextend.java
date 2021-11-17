package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:41
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lccontextend implements KLEntity{
    public String contno;
    public String operatesource;
    public String back1;
    public String back2;
    public String back3;
    public String back4;
    public String policyflag;
    public String preformancecom;
    public String back5;
    public String back6;
    public String back7;
    public String back8;
    public String back9;
    public String back10;
    public String back11;
    public String back12;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Lccontextend\":{"
                + "\"contno\":\""
                + contno + '\"'
                + ",\"operatesource\":\""
                + operatesource + '\"'
                + ",\"back1\":\""
                + back1 + '\"'
                + ",\"back2\":\""
                + back2 + '\"'
                + ",\"back3\":\""
                + back3 + '\"'
                + ",\"back4\":\""
                + back4 + '\"'
                + ",\"policyflag\":\""
                + policyflag + '\"'
                + ",\"preformancecom\":\""
                + preformancecom + '\"'
                + ",\"back5\":\""
                + back5 + '\"'
                + ",\"back6\":\""
                + back6 + '\"'
                + ",\"back7\":\""
                + back7 + '\"'
                + ",\"back8\":\""
                + back8 + '\"'
                + ",\"back9\":\""
                + back9 + '\"'
                + ",\"back10\":\""
                + back10 + '\"'
                + ",\"back11\":\""
                + back11 + '\"'
                + ",\"back12\":\""
                + back12 + '\"'
                + ",\"etl_dt\":\""
                + etl_dt + '\"'
                + ",\"etl_tm\":\""
                + etl_tm + '\"'
                + ",\"etl_fg\":\""
                + etl_fg + '\"'
                + ",\"op_ts\":\""
                + op_ts + '\"'
                + ",\"current_ts\":\""
                + current_ts + '\"'
                + ",\"load_date\":\""
                + load_date + '\"'
                + "}}";

    }
}
