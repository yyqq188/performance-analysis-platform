package com.pactera.yhl.apps.develop.premiums.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class PremiumsKafkaEntity01 implements Serializable {
    public String workarea;
    public String managecom;
    public String prem;
}
