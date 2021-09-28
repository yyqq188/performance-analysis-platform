package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 17:08
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class t01branchinfo {
    public String branch_id_parent;
    public String branch_id;
    public String branch_id_full;
    public String leaf;
    public Integer class_id;
    public Integer grade_id;
    public String financial_right;
    public Integer type_id;
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
    public BigDecimal interest_tax_rate;
    public String group_telephone1;
    public String group_telephone2;
    public String bank_telephone1;
    public String bank_telephone2;
    public String telephone2;
    public Integer commision_standard;
    public Integer assess_standard;
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
}
