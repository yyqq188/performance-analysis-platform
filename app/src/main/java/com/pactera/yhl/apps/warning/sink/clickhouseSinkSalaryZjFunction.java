package com.pactera.yhl.apps.warning.sink;

import com.pactera.yhl.apps.warning.entity.AssessmentResult;
import com.pactera.yhl.apps.warning.entity.GeneralResult;

/**
 * @author SUN KI
 * @time 2021/11/24 19:40
 * @Desc
 */
public class clickhouseSinkSalaryZjFunction extends AbstractCKSinkSQ<AssessmentResult> {
    public clickhouseSinkSalaryZjFunction(){
        tableName = "APPLICATION_ASSESSMENT_SALARY_RESULT_FQZJ";
    }
    @Override
    public void invoke(AssessmentResult value, Context context) throws Exception {
        super.invoke(value,context);
//        Field[] declaredFields = value.getClass().getDeclaredFields();
//        List<String> arrs = new ArrayList<>();
//        for(Field f:declaredFields){
//            Object o = f.get(value);
//            System.out.println(o);
//            try{
//                arrs.add(o.toString());
//            }catch (Exception e){
//                arrs.add("null");
//            }
//
//        }
//        sql = String.format("insert into " + tableName + " values (\'%s\')",
//                String.join("\',\'",arrs));
//        statement.executeUpdate(sql);
//        System.out.println(sql);
    }
}
