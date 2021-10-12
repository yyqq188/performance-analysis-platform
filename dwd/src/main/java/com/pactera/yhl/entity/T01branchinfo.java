package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 17:08
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class T01branchinfo implements KLEntity{
    public String branch_id_parent;
    public String branch_id;
    public String branch_id_full;
    public String leaf;
    public String class_id;
    public String grade_id;
    public String financial_right;
    public String type_id;
    public String branch_name;
    public String abbr_name;
    public String found_date;
    public String recall_date;
    public String status;
    public String delegate;
    public String tax_code;
    public String zip;
    public String address;
    public String area_code;
    public String telephone;
    public String fax;
    public String email;
    public String chief_claim;
    public String chief_group_uw;
    public String chief_indivi_uw;
    public String chief_claim_grp;
    public String chief_claim_health;
    public String insert_time;
    public String update_time;
    public String is_base;
    public String leader_id;
    public String country_code;
    public String interest_tax_rate;
    public String group_telephone1;
    public String group_telephone2;
    public String bank_telephone1;
    public String bank_telephone2;
    public String telephone2;
    public String commision_standard;
    public String assess_standard;
    public String branch_name_smisabbr;
    public String branch_id_parentsale;
    public String remark;
    public String is_basic;
    public String is_startup;
    public String version_id;
    public String branch_id_query;
    public String user_id;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"T01branchinfo\":{"
                + "\"branch_id_parent\":\""
                + branch_id_parent + '\"'
                + ",\"branch_id\":\""
                + branch_id + '\"'
                + ",\"branch_id_full\":\""
                + branch_id_full + '\"'
                + ",\"leaf\":\""
                + leaf + '\"'
                + ",\"class_id\":\""
                + class_id + '\"'
                + ",\"grade_id\":\""
                + grade_id + '\"'
                + ",\"financial_right\":\""
                + financial_right + '\"'
                + ",\"type_id\":\""
                + type_id + '\"'
                + ",\"branch_name\":\""
                + branch_name + '\"'
                + ",\"abbr_name\":\""
                + abbr_name + '\"'
                + ",\"found_date\":\""
                + found_date + '\"'
                + ",\"recall_date\":\""
                + recall_date + '\"'
                + ",\"status\":\""
                + status + '\"'
                + ",\"delegate\":\""
                + delegate + '\"'
                + ",\"tax_code\":\""
                + tax_code + '\"'
                + ",\"zip\":\""
                + zip + '\"'
                + ",\"address\":\""
                + address + '\"'
                + ",\"area_code\":\""
                + area_code + '\"'
                + ",\"telephone\":\""
                + telephone + '\"'
                + ",\"fax\":\""
                + fax + '\"'
                + ",\"email\":\""
                + email + '\"'
                + ",\"chief_claim\":\""
                + chief_claim + '\"'
                + ",\"chief_group_uw\":\""
                + chief_group_uw + '\"'
                + ",\"chief_indivi_uw\":\""
                + chief_indivi_uw + '\"'
                + ",\"chief_claim_grp\":\""
                + chief_claim_grp + '\"'
                + ",\"chief_claim_health\":\""
                + chief_claim_health + '\"'
                + ",\"insert_time\":\""
                + insert_time + '\"'
                + ",\"update_time\":\""
                + update_time + '\"'
                + ",\"is_base\":\""
                + is_base + '\"'
                + ",\"leader_id\":\""
                + leader_id + '\"'
                + ",\"country_code\":\""
                + country_code + '\"'
                + ",\"interest_tax_rate\":\""
                + interest_tax_rate + '\"'
                + ",\"group_telephone1\":\""
                + group_telephone1 + '\"'
                + ",\"group_telephone2\":\""
                + group_telephone2 + '\"'
                + ",\"bank_telephone1\":\""
                + bank_telephone1 + '\"'
                + ",\"bank_telephone2\":\""
                + bank_telephone2 + '\"'
                + ",\"telephone2\":\""
                + telephone2 + '\"'
                + ",\"commision_standard\":\""
                + commision_standard + '\"'
                + ",\"assess_standard\":\""
                + assess_standard + '\"'
                + ",\"branch_name_smisabbr\":\""
                + branch_name_smisabbr + '\"'
                + ",\"branch_id_parentsale\":\""
                + branch_id_parentsale + '\"'
                + ",\"remark\":\""
                + remark + '\"'
                + ",\"is_basic\":\""
                + is_basic + '\"'
                + ",\"is_startup\":\""
                + is_startup + '\"'
                + ",\"version_id\":\""
                + version_id + '\"'
                + ",\"branch_id_query\":\""
                + branch_id_query + '\"'
                + ",\"user_id\":\""
                + user_id + '\"'
                + ",\"op_ts\":\""
                + op_ts + '\"'
                + ",\"current_ts\":\""
                + current_ts + '\"'
                + ",\"load_date\":\""
                + load_date + '\"'
                + "}}";

    }
}
