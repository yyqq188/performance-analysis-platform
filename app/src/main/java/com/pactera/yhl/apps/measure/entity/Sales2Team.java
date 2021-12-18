package com.pactera.yhl.apps.measure.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sales2Team {
    public double slf_prem;
    public double team_prem;
    public String sales_id;


    public String department_agentcode;
    public String department_code;
    public String department_name;

    public String district_agentcode;
    public String district_code;
    public String district_name;

    public String is_change;
}
