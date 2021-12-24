package com.pactera.yhl.transform.normal.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:54
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lmedoritem implements KLEntity {
    public String edorcode;
    public String edorname;
    public String appobj;
    public String displayflag;
    public String calflag;
    public String needdetail;
    public String grpneedlist;
    public String edorpopedom;
    public String edortypeflag;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Lmedoritem\":{"
                + "\"edorcode\":\""
                + edorcode + '\"'
                + ",\"edorname\":\""
                + edorname + '\"'
                + ",\"appobj\":\""
                + appobj + '\"'
                + ",\"displayflag\":\""
                + displayflag + '\"'
                + ",\"calflag\":\""
                + calflag + '\"'
                + ",\"needdetail\":\""
                + needdetail + '\"'
                + ",\"grpneedlist\":\""
                + grpneedlist + '\"'
                + ",\"edorpopedom\":\""
                + edorpopedom + '\"'
                + ",\"edortypeflag\":\""
                + edortypeflag + '\"'
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
