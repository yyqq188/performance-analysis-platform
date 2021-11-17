package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:04
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Laagentcertif implements KLEntity {
    public String agency_sales_id;
    public String bank_id;
    public String channel_id;
    public String agency_sales_name;
    public String develop_card;
    public String developcard_start_date;
    public String developcard_end_date;
    public String insert_data;
    public String stateflag;
    public String idno;
    public String phone;
    public String mobile;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Laagentcertif\":{"
                + "\"agency_sales_id\":\""
                + agency_sales_id + '\"'
                + ",\"bank_id\":\""
                + bank_id + '\"'
                + ",\"channel_id\":\""
                + channel_id + '\"'
                + ",\"agency_sales_name\":\""
                + agency_sales_name + '\"'
                + ",\"develop_card\":\""
                + develop_card + '\"'
                + ",\"developcard_start_date\":\""
                + developcard_start_date + '\"'
                + ",\"developcard_end_date\":\""
                + developcard_end_date + '\"'
                + ",\"insert_data\":\""
                + insert_data + '\"'
                + ",\"stateflag\":\""
                + stateflag + '\"'
                + ",\"idno\":\""
                + idno + '\"'
                + ",\"phone\":\""
                + phone + '\"'
                + ",\"mobile\":\""
                + mobile + '\"'
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
