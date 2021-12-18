package com.pactera.yhl.apps.warning.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author SUN KI
 * @Description
 * @create 2021/11/16 14:22
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AssessmentResult {
    public String key_id;
    public String day_id;
    public String manage_code;
    public String manage_name;
    public String agent_grade;
    public String manpower_assessment_month;
    public String full_manpower_month;
    public String f_manpower_rate_month;
//    public String other_manpower_month;
    public String zero_manpower_month;
    public String z_manpower_rate_month;
    public String load_date;
}
