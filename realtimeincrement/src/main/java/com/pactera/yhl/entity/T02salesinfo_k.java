package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 17:06
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class T02salesinfo_k implements KLEntity {
    public String channel_id;
    public String channel_id_2nd;
    public String branch_id;
    public String workspace_id;
    public String team_id;
    public String leader_id;
    public String recommend_type;
    public String recommend_id;
    public String recommend_name;
    public String is_leader_reserve;
    public String sales_id;
    public String workload_goal;
    public String employ_kind;
    public String bank_biz_years;
    public String instancy_linkman;
    public String instancy_linkman_telephon;
    public String instancy_linkman_relation;
    public String recruit_source;
    public String sales_name_once;
    public String server_com_once;
    public String channel_type_sale;
    public String group_biz_years;
    public String sales_type;
    public String sales_type_rank;
    public String sales_name;
    public String id_type;
    public String id_no;
    public String sex;
    public String birthday;
    public String nation;
    public String education;
    public String political_stat;
    public String marital_stat;
    public String sales_native_2lvl;
    public String sales_native_3lvl;
    public String domicile;
    public String home_address;
    public String home_zipcode;
    public String area_type;
    public String rank;
    public String duty_date;
    public String probation_date;
    public String assess_start_date;
    public String deposit;
    public String mobile;
    public String fixed_line;
    public String email;
    public String phs;
    public String is_criminal_record;
    public String is_qualicert;
    public String server_com_num;
    public String stat;
    public String is_resigned;
    public String is_green_passport;
    public String is_same_vocation;
    public String work_experience;
    public String health_stat;
    public String sales_stat;
    public String is_full_time;
    public String is_get_annuity;
    public String is_get_msg;
    public String graduate_school;
    public String degree;
    public String major;
    public String technical_title;
    public String speciality;
    public String foreign_language;
    public String foreign_language_lvl;
    public String computer_lvl;
    public String old_job;
    public String old_company;
    public String work_date;
    public String vocation_title;
    public String remark_1;
    public String remark_2;
    public String remark_3;
    public String remark;
    public String insert_time;
    public String operate_type;
    public String user_id;
    public String biz_years;
    public String enter_rank;
    public String to_director_date;
    public String dismiss_date_pre;
    public String dismiss_date;
    public String server_com_once2;
    public String channel_type_reason;
    public String avoid_relegation_times;
    public String remission_start_date;
    public String remission_type;
    public String mountguard_date;
    public String dismiss_times;
    public String salary_rank;
    public String comp_sales_id;
    public String comp_branch_id;
    public String version_id;
    public String yb_stat;
    public String manager_rank;
    public String manager_duty_date;
    public String sales_id_old;
    public String multiinsscore;
    public String agency_agreement;
    public String agreement_start;
    public String agreement_end;
    public String qudao_id;
    public String salesattribute;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String hobproflag;
    public String modify_timestamp;
    public String isnet;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"T02salesinfo_k\":{"
                + "\"channel_id\":\""
                + channel_id + '\"'
                + ",\"channel_id_2nd\":\""
                + channel_id_2nd + '\"'
                + ",\"branch_id\":\""
                + branch_id + '\"'
                + ",\"workspace_id\":\""
                + workspace_id + '\"'
                + ",\"team_id\":\""
                + team_id + '\"'
                + ",\"leader_id\":\""
                + leader_id + '\"'
                + ",\"recommend_type\":\""
                + recommend_type + '\"'
                + ",\"recommend_id\":\""
                + recommend_id + '\"'
                + ",\"recommend_name\":\""
                + recommend_name + '\"'
                + ",\"is_leader_reserve\":\""
                + is_leader_reserve + '\"'
                + ",\"sales_id\":\""
                + sales_id + '\"'
                + ",\"workload_goal\":\""
                + workload_goal + '\"'
                + ",\"employ_kind\":\""
                + employ_kind + '\"'
                + ",\"bank_biz_years\":\""
                + bank_biz_years + '\"'
                + ",\"instancy_linkman\":\""
                + instancy_linkman + '\"'
                + ",\"instancy_linkman_telephon\":\""
                + instancy_linkman_telephon + '\"'
                + ",\"instancy_linkman_relation\":\""
                + instancy_linkman_relation + '\"'
                + ",\"recruit_source\":\""
                + recruit_source + '\"'
                + ",\"sales_name_once\":\""
                + sales_name_once + '\"'
                + ",\"server_com_once\":\""
                + server_com_once + '\"'
                + ",\"channel_type_sale\":\""
                + channel_type_sale + '\"'
                + ",\"group_biz_years\":\""
                + group_biz_years + '\"'
                + ",\"sales_type\":\""
                + sales_type + '\"'
                + ",\"sales_type_rank\":\""
                + sales_type_rank + '\"'
                + ",\"sales_name\":\""
                + sales_name + '\"'
                + ",\"id_type\":\""
                + id_type + '\"'
                + ",\"id_no\":\""
                + id_no + '\"'
                + ",\"sex\":\""
                + sex + '\"'
                + ",\"birthday\":\""
                + birthday + '\"'
                + ",\"nation\":\""
                + nation + '\"'
                + ",\"education\":\""
                + education + '\"'
                + ",\"political_stat\":\""
                + political_stat + '\"'
                + ",\"marital_stat\":\""
                + marital_stat + '\"'
                + ",\"sales_native_2lvl\":\""
                + sales_native_2lvl + '\"'
                + ",\"sales_native_3lvl\":\""
                + sales_native_3lvl + '\"'
                + ",\"domicile\":\""
                + domicile + '\"'
                + ",\"home_address\":\""
                + home_address + '\"'
                + ",\"home_zipcode\":\""
                + home_zipcode + '\"'
                + ",\"area_type\":\""
                + area_type + '\"'
                + ",\"rank\":\""
                + rank + '\"'
                + ",\"duty_date\":\""
                + duty_date + '\"'
                + ",\"probation_date\":\""
                + probation_date + '\"'
                + ",\"assess_start_date\":\""
                + assess_start_date + '\"'
                + ",\"deposit\":\""
                + deposit + '\"'
                + ",\"mobile\":\""
                + mobile + '\"'
                + ",\"fixed_line\":\""
                + fixed_line + '\"'
                + ",\"email\":\""
                + email + '\"'
                + ",\"phs\":\""
                + phs + '\"'
                + ",\"is_criminal_record\":\""
                + is_criminal_record + '\"'
                + ",\"is_qualicert\":\""
                + is_qualicert + '\"'
                + ",\"server_com_num\":\""
                + server_com_num + '\"'
                + ",\"stat\":\""
                + stat + '\"'
                + ",\"is_resigned\":\""
                + is_resigned + '\"'
                + ",\"is_green_passport\":\""
                + is_green_passport + '\"'
                + ",\"is_same_vocation\":\""
                + is_same_vocation + '\"'
                + ",\"work_experience\":\""
                + work_experience + '\"'
                + ",\"health_stat\":\""
                + health_stat + '\"'
                + ",\"sales_stat\":\""
                + sales_stat + '\"'
                + ",\"is_full_time\":\""
                + is_full_time + '\"'
                + ",\"is_get_annuity\":\""
                + is_get_annuity + '\"'
                + ",\"is_get_msg\":\""
                + is_get_msg + '\"'
                + ",\"graduate_school\":\""
                + graduate_school + '\"'
                + ",\"degree\":\""
                + degree + '\"'
                + ",\"major\":\""
                + major + '\"'
                + ",\"technical_title\":\""
                + technical_title + '\"'
                + ",\"speciality\":\""
                + speciality + '\"'
                + ",\"foreign_language\":\""
                + foreign_language + '\"'
                + ",\"foreign_language_lvl\":\""
                + foreign_language_lvl + '\"'
                + ",\"computer_lvl\":\""
                + computer_lvl + '\"'
                + ",\"old_job\":\""
                + old_job + '\"'
                + ",\"old_company\":\""
                + old_company + '\"'
                + ",\"work_date\":\""
                + work_date + '\"'
                + ",\"vocation_title\":\""
                + vocation_title + '\"'
                + ",\"remark_1\":\""
                + remark_1 + '\"'
                + ",\"remark_2\":\""
                + remark_2 + '\"'
                + ",\"remark_3\":\""
                + remark_3 + '\"'
                + ",\"remark\":\""
                + remark + '\"'
                + ",\"insert_time\":\""
                + insert_time + '\"'
                + ",\"operate_type\":\""
                + operate_type + '\"'
                + ",\"user_id\":\""
                + user_id + '\"'
                + ",\"biz_years\":\""
                + biz_years + '\"'
                + ",\"enter_rank\":\""
                + enter_rank + '\"'
                + ",\"to_director_date\":\""
                + to_director_date + '\"'
                + ",\"dismiss_date_pre\":\""
                + dismiss_date_pre + '\"'
                + ",\"dismiss_date\":\""
                + dismiss_date + '\"'
                + ",\"server_com_once2\":\""
                + server_com_once2 + '\"'
                + ",\"channel_type_reason\":\""
                + channel_type_reason + '\"'
                + ",\"avoid_relegation_times\":\""
                + avoid_relegation_times + '\"'
                + ",\"remission_start_date\":\""
                + remission_start_date + '\"'
                + ",\"remission_type\":\""
                + remission_type + '\"'
                + ",\"mountguard_date\":\""
                + mountguard_date + '\"'
                + ",\"dismiss_times\":\""
                + dismiss_times + '\"'
                + ",\"salary_rank\":\""
                + salary_rank + '\"'
                + ",\"comp_sales_id\":\""
                + comp_sales_id + '\"'
                + ",\"comp_branch_id\":\""
                + comp_branch_id + '\"'
                + ",\"version_id\":\""
                + version_id + '\"'
                + ",\"yb_stat\":\""
                + yb_stat + '\"'
                + ",\"manager_rank\":\""
                + manager_rank + '\"'
                + ",\"manager_duty_date\":\""
                + manager_duty_date + '\"'
                + ",\"sales_id_old\":\""
                + sales_id_old + '\"'
                + ",\"multiinsscore\":\""
                + multiinsscore + '\"'
                + ",\"agency_agreement\":\""
                + agency_agreement + '\"'
                + ",\"agreement_start\":\""
                + agreement_start + '\"'
                + ",\"agreement_end\":\""
                + agreement_end + '\"'
                + ",\"qudao_id\":\""
                + qudao_id + '\"'
                + ",\"salesattribute\":\""
                + salesattribute + '\"'
                + ",\"etl_dt\":\""
                + etl_dt + '\"'
                + ",\"etl_tm\":\""
                + etl_tm + '\"'
                + ",\"etl_fg\":\""
                + etl_fg + '\"'
                + ",\"hobproflag\":\""
                + hobproflag + '\"'
                + ",\"modify_timestamp\":\""
                + modify_timestamp + '\"'
                + ",\"isnet\":\""
                + isnet + '\"'
                + ",\"op_ts\":\""
                + op_ts + '\"'
                + ",\"current_ts\":\""
                + current_ts + '\"'
                + ",\"load_date\":\""
                + load_date + '\"'
                + "}}";

    }
}
