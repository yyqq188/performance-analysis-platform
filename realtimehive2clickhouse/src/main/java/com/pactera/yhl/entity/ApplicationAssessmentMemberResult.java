package com.pactera.yhl.entity;

import java.io.Serializable;

public class ApplicationAssessmentMemberResult implements KLEntity, Serializable {
    public String key_id;
    public String day_id;
    public String manage_code;
    public String manage_name;
    public String agent_grade;
    public String member_assessment_quarter;
    public String member_advance_quarter;
    public String member_keep_quarter;
    public String member_degrade_quarter;
    public String member_degrade_rate_quarter;
    public String load_date;
}
