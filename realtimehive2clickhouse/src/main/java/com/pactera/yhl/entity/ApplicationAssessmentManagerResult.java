package com.pactera.yhl.entity;

import java.io.Serializable;

public class ApplicationAssessmentManagerResult implements KLEntity, Serializable {
    public String key_id;
    public String day_id;
    public String manage_code;
    public String manage_name;
    public String agent_grade;
    public String manager_assessment_quarter;
    public String manager_advance_quarter;
    public String manager_keep_quarter;
    public String manager_degrade_quarter;
    public String manager_degrade_rate_quarter;
    public String load_date;
}
