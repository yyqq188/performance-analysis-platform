package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 17:02
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class T01teaminfo implements KLEntity {
    public String channel_id;
    public String branch_id;
    public String workspace_id;
    public String team_id_parent;
    public String team_id;
    public String team_name;
    public String team_lvl;
    public String leader_id;
    public String leader_id_reserve;
    public String startup_date;
    public String stat;
    public String address;
    public String zipcode;
    public String telephone;
    public String fax;
    public String team_id_old;
    public String found_date;
    public String cancel_date;
    public String cancel_reason;
    public String sales_goal;
    public String remark;
    public String insert_time;
    public String user_id;
    public String operate_type;
    public String team_type;
    public String channel_id_2nd;
    public String leader_position_date;
    public String versionid;
    public String team_id_parent_old;
    public String team_lvl_old;
    public String leader_type;
    public String manager_name;
    public String team_name_system;
    public String is_inherency;
    public String reserve_date;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"T01teaminfo\":{"
                + "\"channel_id\":\""
                + channel_id + '\"'
                + ",\"branch_id\":\""
                + branch_id + '\"'
                + ",\"workspace_id\":\""
                + workspace_id + '\"'
                + ",\"team_id_parent\":\""
                + team_id_parent + '\"'
                + ",\"team_id\":\""
                + team_id + '\"'
                + ",\"team_name\":\""
                + team_name + '\"'
                + ",\"team_lvl\":\""
                + team_lvl + '\"'
                + ",\"leader_id\":\""
                + leader_id + '\"'
                + ",\"leader_id_reserve\":\""
                + leader_id_reserve + '\"'
                + ",\"startup_date\":\""
                + startup_date + '\"'
                + ",\"stat\":\""
                + stat + '\"'
                + ",\"address\":\""
                + address + '\"'
                + ",\"zipcode\":\""
                + zipcode + '\"'
                + ",\"telephone\":\""
                + telephone + '\"'
                + ",\"fax\":\""
                + fax + '\"'
                + ",\"team_id_old\":\""
                + team_id_old + '\"'
                + ",\"found_date\":\""
                + found_date + '\"'
                + ",\"cancel_date\":\""
                + cancel_date + '\"'
                + ",\"cancel_reason\":\""
                + cancel_reason + '\"'
                + ",\"sales_goal\":\""
                + sales_goal + '\"'
                + ",\"remark\":\""
                + remark + '\"'
                + ",\"insert_time\":\""
                + insert_time + '\"'
                + ",\"user_id\":\""
                + user_id + '\"'
                + ",\"operate_type\":\""
                + operate_type + '\"'
                + ",\"team_type\":\""
                + team_type + '\"'
                + ",\"channel_id_2nd\":\""
                + channel_id_2nd + '\"'
                + ",\"leader_position_date\":\""
                + leader_position_date + '\"'
                + ",\"versionid\":\""
                + versionid + '\"'
                + ",\"team_id_parent_old\":\""
                + team_id_parent_old + '\"'
                + ",\"team_lvl_old\":\""
                + team_lvl_old + '\"'
                + ",\"leader_type\":\""
                + leader_type + '\"'
                + ",\"manager_name\":\""
                + manager_name + '\"'
                + ",\"team_name_system\":\""
                + team_name_system + '\"'
                + ",\"is_inherency\":\""
                + is_inherency + '\"'
                + ",\"reserve_date\":\""
                + reserve_date + '\"'
                + ",\"op_ts\":\""
                + op_ts + '\"'
                + ",\"current_ts\":\""
                + current_ts + '\"'
                + ",\"load_date\":\""
                + load_date + '\"'
                + "}}";

    }
}
