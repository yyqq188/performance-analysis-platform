package com.pactera.yhl.apps.measure.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AssessResult {
    public String key_id;
    public String day_id;
    public String agent_id;
    public String agent_name;
    public String provincecom_code;
    public String provincecom_name;
    public String workarea;
    public String hire_month;
    public String agentstate;
    public String department_code;
    public String department_name;
    public String department_agentcode;
    public String department_leader;
    public String district_code;
    public String district_name;
    public String district_agentcode;
    public String district_leader;
    public String assessmonth;
    public String agent_grade;
    public String to_agent_grade;
    public String d_fyp;
    public String m_fyp;
    public String q_fyp;
    public String member_rolling_rate;
    public String department_d_fyp;
    public String department_m_fyp;
    public String department_q_fyp;
    public String department_assessment;
    public String department_assessment_m;
    public String department_activity_m;
    public String department_activity_q;
    public String department_activity_rate_m;
    public String department_activity_rate_q;
    public String department_rolling_rate;
    public String distinct_d_fyp;
    public String distinct_m_fyp;
    public String distinct_q_fyp;
    public String distinct_assessment;
    public String distinct_assessment_m;
    public String distinct_activity_m;
    public String distinct_activity_q;
    public String distinct_activity_rate_m;
    public String distinct_activity_rate_q;
    public String distinct_rolling_rate;
    public String assessment_standard;
    public String assessment_difference;
    public String assessment_result;
    public String load_date;
}
