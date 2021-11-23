package com.pactera.yhl.apps.develop.premiums.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DimensionT01BranchId {
    public String branch_id;
    public String branch_id_parent;
    public String branch_id_full;
    public String branch_name;
}
