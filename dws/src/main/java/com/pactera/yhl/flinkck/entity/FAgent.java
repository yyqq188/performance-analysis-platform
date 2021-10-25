package com.pactera.yhl.flinkck.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FAgent {
    public String agent_code;
    public String agent_name;
    public String agent_state;
    public String init_agentgrade;
    public String agent_grade;
    public String channel_id;
    public String certi_type;
    public String certi_code;
    public String education_id;
    public String mobile;
    public String fixed_line;
    public String email;
    public String recommend_type;
    public String recommend_id;
    public String recommend_name;
    public String is_resigned;
    public String channel_type_reason;
    public String avoid_relegation_times;
    public String remission_start_date;
    public String remission_type;
    public String workarea;
    public String department_code;
    public String department_name;
    public String department_leader;
    public String district_code;
    public String district_name;
    public String district_leader;
    public String branch_id3;
    public String branch_name3;
    public String branch_id4;
    public String branch_name4;
    public String gender;
    public String hire_date;
    public String hire_month;
    public String leave_date;
    public String assess_start_date;

}
