package com.pactera.yhl.apps.measure.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/30 18:47
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Measure1 {
    public double slf_prem;
    public double team_prem;
    public String sales_id;
    public String branch_id;
    public String team_id;
    public String rank;
    public int labor;

    public String hire_date;
    public String sales_name;
    public String stat;
    public String workarea;

    public String department_agentcode;
    public String department_leader;
    public String department_code;
    public String department_name;

    public String department_m_fyp;
    public String department_q_fyp;
    public String department_assessment;
    public String department_activity_m;
    public String department_activity_q;

    public String district_agentcode;
    public String district_leader;
    public String district_code;
    public String district_name;

    public String distinct_m_fyp;
    public String distinct_q_fyp;
    public String distinct_assessment;
    public String distinct_activity_m;
    public String distinct_activity_q;

    public String is_change;
}
