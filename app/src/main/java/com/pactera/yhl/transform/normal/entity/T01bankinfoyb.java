package com.pactera.yhl.transform.normal.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 15:47
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class T01bankinfoyb implements KLEntity {
    public String bank_id_parent;
    public String bank_id;
    public String bank_id_full;
    public String class_id;
    public String grade_id;
    public String bank_type;
    public String bank_name;
    public String found_date;
    public String zip;
    public String address;
    public String area_code;
    public String telephone;
    public String email;
    public String leader_id;
    public String commision_standard;
    public String is_startup;
    public String bank_id_query;
    public String branchid;
    public String sales_qualify;
    public String agreement_id;
    public String agreement_startdate;
    public String agreement_enddate;
    public String user_id;
    public String network;
    public String type_id;
    public String manager;
    public String leader_telphone;
    public String bank_name_full;
    public String certificate;
    public String certificate_start_date;
    public String certificate_end_date;
    public String cagencyid;
    public String cagencyprovince;
    public String cagencycity;
    public String cagencycopoid;
    public String cagencybank;
    public String cagencysecbank;
    public String cagencyname;
    public String channel_type;
    public String channel_id;
    public String isnot_cooperation;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"T01bankinfoyb\":{"
                + "\"bank_id_parent\":\""
                + bank_id_parent + '\"'
                + ",\"bank_id\":\""
                + bank_id + '\"'
                + ",\"bank_id_full\":\""
                + bank_id_full + '\"'
                + ",\"class_id\":\""
                + class_id + '\"'
                + ",\"grade_id\":\""
                + grade_id + '\"'
                + ",\"bank_type\":\""
                + bank_type + '\"'
                + ",\"bank_name\":\""
                + bank_name + '\"'
                + ",\"found_date\":\""
                + found_date + '\"'
                + ",\"zip\":\""
                + zip + '\"'
                + ",\"address\":\""
                + address + '\"'
                + ",\"area_code\":\""
                + area_code + '\"'
                + ",\"telephone\":\""
                + telephone + '\"'
                + ",\"email\":\""
                + email + '\"'
                + ",\"leader_id\":\""
                + leader_id + '\"'
                + ",\"commision_standard\":\""
                + commision_standard + '\"'
                + ",\"is_startup\":\""
                + is_startup + '\"'
                + ",\"bank_id_query\":\""
                + bank_id_query + '\"'
                + ",\"branchid\":\""
                + branchid + '\"'
                + ",\"sales_qualify\":\""
                + sales_qualify + '\"'
                + ",\"agreement_id\":\""
                + agreement_id + '\"'
                + ",\"agreement_startdate\":\""
                + agreement_startdate + '\"'
                + ",\"agreement_enddate\":\""
                + agreement_enddate + '\"'
                + ",\"user_id\":\""
                + user_id + '\"'
                + ",\"network\":\""
                + network + '\"'
                + ",\"type_id\":\""
                + type_id + '\"'
                + ",\"manager\":\""
                + manager + '\"'
                + ",\"leader_telphone\":\""
                + leader_telphone + '\"'
                + ",\"bank_name_full\":\""
                + bank_name_full + '\"'
                + ",\"certificate\":\""
                + certificate + '\"'
                + ",\"certificate_start_date\":\""
                + certificate_start_date + '\"'
                + ",\"certificate_end_date\":\""
                + certificate_end_date + '\"'
                + ",\"cagencyid\":\""
                + cagencyid + '\"'
                + ",\"cagencyprovince\":\""
                + cagencyprovince + '\"'
                + ",\"cagencycity\":\""
                + cagencycity + '\"'
                + ",\"cagencycopoid\":\""
                + cagencycopoid + '\"'
                + ",\"cagencybank\":\""
                + cagencybank + '\"'
                + ",\"cagencysecbank\":\""
                + cagencysecbank + '\"'
                + ",\"cagencyname\":\""
                + cagencyname + '\"'
                + ",\"channel_type\":\""
                + channel_type + '\"'
                + ",\"channel_id\":\""
                + channel_id + '\"'
                + ",\"isnot_cooperation\":\""
                + isnot_cooperation + '\"'
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
