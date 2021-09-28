package entity;

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
public class t01bankinfoyb {
    public String bank_id_parent;
    public String bank_id;
    public String bank_id_full;
    public Integer class_id;
    public Integer grade_id;
    public String bank_type;
    public String bank_name;
    public String found_date;
    public String zip;
    public String address;
    public String area_code;
    public String telephone;
    public String email;
    public String leader_id;
    public Integer commision_standard;
    public String is_startup;
    public String bank_id_query;
    public String branchid;
    public String sales_qualify;
    public String agreement_id;
    public String agreement_startdate;
    public String agreement_enddate;
    public String user_id;
    public String network;
    public Integer type_id;
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
}
