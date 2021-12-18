package com.pactera.yhl.apps.construction.util;

import org.apache.flink.api.common.state.MapState;
import org.apache.hadoop.hbase.client.HTable;

import static com.pactera.yhl.util.Util.getHbaseValue;

/**
 * @author: TSY
 * @create: 2021/12/1 0001 上午 10:22
 * @description: 分公司计算中间状态核心代码
 */
public class TotalStateStorage {

    /**
     * 日期缴活动人力中间状态存储
     * 日合格人力中间状态存储
     * @param sumState
     * @param countState
     * @param key_id
     * @param sales_id
     * @param currentPrem
     * @param dou
     * @throws Exception
     */
    public static synchronized void dayStateTotal(MapState<String, Double> sumState, MapState<String, Integer> countState, String key_id, String sales_id, Double currentPrem, Double dou) throws Exception {
        //存在sales_id
        if (sumState.contains(sales_id)) {
            Double historyPrem = sumState.get(sales_id);
            double sum = historyPrem + currentPrem;
            //大于等于
            if (historyPrem >= dou) {
                sumState.put(sales_id, sum);
            }else {
                if(sum >= dou){
                    sumState.put(sales_id, sum);
                    countState.put(key_id, countState.get(key_id) + 1);
                }else {
                    sumState.put(sales_id, sum);
                }
            }
        } else {
            //不存在sales_id
            //存在日期加机构
            if (countState.contains(key_id)) {
                sumState.put(sales_id, currentPrem);
                if(currentPrem >= dou){
                    countState.put(key_id, countState.get(key_id) + 1);
                }else {
                    countState.put(key_id, countState.get(key_id));
                }
                //不存在sales_id
                //不存在日期加机构
            } else {
                sumState.put(sales_id, currentPrem);
                if(currentPrem >= dou){
                    countState.put(key_id, 1);
                }else {
                    countState.put(key_id, 0);
                }
            }
        }
    }

    /**
     * 日入职出单人力中间状态存储
     * @param sumState
     * @param countState
     * @param key_id
     * @param sales_id
     * @param currentPrem
     * @param dou
     * @throws Exception
     */
    public static synchronized void dayEnterStateTotal(MapState<String, Double> sumState, MapState<String, Integer> countState, String key_id, String sales_id, Double currentPrem, Double dou) throws Exception {
        //存在sales_id
        if (sumState.contains(sales_id)) {
            Double historyPrem = sumState.get(sales_id);
            double sum = historyPrem + currentPrem;
            //大于等于1W
            if (historyPrem > dou) {
                sumState.put(sales_id, sum);
            }else {
                if(sum > dou){
                    sumState.put(sales_id, sum);
                    countState.put(key_id, countState.get(key_id) + 1);
                }else {
                    sumState.put(sales_id, sum);
                }
            }
        } else {
            //不存在sales_id
            //存在日期加机构
            if (countState.contains(key_id)) {
                sumState.put(sales_id, currentPrem);
                if(currentPrem > dou){
                    countState.put(key_id, countState.get(key_id) + 1);
                }else {
                    countState.put(key_id, countState.get(key_id));
                }
                //不存在sales_id
                //不存在日期加机构
            } else {
                sumState.put(sales_id, currentPrem);
                if(currentPrem > dou){
                    countState.put(key_id, 1);
                }else {
                    countState.put(key_id, 0);
                }
            }
        }
    }

    /**
     * 保存当前累计期缴保费
     * @param sumState
     * @param key_id
     * @param currentPrem
     * @throws Exception
     */
    public static synchronized void sumStatePremiumTotal(MapState<String, Double> sumState,String key_id,Double currentPrem) throws Exception {
        if(sumState.contains(key_id)){
            sumState.put(key_id, Double.valueOf(String.format("%.2f",sumState.get(key_id) + currentPrem)));
        }else {
            sumState.put(key_id, currentPrem);
        }
    }

    /**
     * 月期缴活动人力
     * @param sumState
     * @param countState
     * @param key_id
     * @param sales_id
     * @param currentPrem
     * @param dou
     * @throws Exception
     */
    public static synchronized void monthActivityStateTotal(MapState<String, Double> sumState, MapState<String, Integer> countState, String key_id, String sales_id, Double currentPrem, HTable hTableManpower_Tmp1Table,Integer activity_manpower_monthManpower, Double dou) throws Exception {
        //存在sales_id
        if (sumState.contains(sales_id)) {
            Double historyPrem = sumState.get(sales_id);
            double sum = Double.valueOf(String.format("%.2f",historyPrem + currentPrem));
            //大于等于1W
            if (historyPrem >= dou) {
                if(sum >= dou ){
                    sumState.put(sales_id, sum);
                }else {
                    sumState.put(sales_id, sum);
                    if(countState.get(key_id) > 0) {
                        countState.put(key_id, countState.get(key_id) - 1);
                    }else {
                        countState.put(key_id, 0);
                    }
                }
                //小于1W
            }else {
                if(sum >= dou ){
                    sumState.put(sales_id, sum);
                    countState.put(key_id, countState.get(key_id) + 1);
                }else {
                    sumState.put(sales_id, sum);
                }
            }
        } else {
            //不存在sales_id
            //存在日期加机构
            if (countState.contains(key_id)) {
                Double ycc = Utils.str2DouIsNull(String.format("%.2f", Double.valueOf(getHbaseValue(hTableManpower_Tmp1Table, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem")) + currentPrem));
                sumState.put(sales_id, ycc);
                //大于等于1W
                if (ycc >= dou) {
                    countState.put(key_id, countState.get(key_id) + 1);
                } else {
                    //小于1W
                    countState.put(key_id, countState.get(key_id));
                }
                //不存在sales_id
                //不存在日期加机构
            } else {
                Double ycc = Utils.str2DouIsNull(String.format("%.2f", Double.valueOf(getHbaseValue(hTableManpower_Tmp1Table, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem")) + currentPrem));
                sumState.put(sales_id, ycc);
                //大于等于1W
                if (ycc >= dou) {
                    countState.put(key_id, activity_manpower_monthManpower + 1);
                } else {
                    //小于1W
                    countState.put(key_id, activity_manpower_monthManpower);
                }
            }
        }
    }

    /**
     *当月入职出单人力
     * @param sumState
     * @param countState
     * @param key_id
     * @param sales_id
     * @param currentPrem
     * @param dou
     * @throws Exception
     */
    public static synchronized void monthIssueStateTotal(MapState<String, Double> sumState, MapState<String, Integer> countState, String key_id, String sales_id, Double currentPrem, HTable hTableManpower_Tmp1Table, Integer enter_order_monthManpower, Double dou) throws Exception {
        //存在sales_id
        if (sumState.contains(sales_id)) {
            Double historyPrem = sumState.get(sales_id);
            double sum = Double.valueOf(String.format("%.2f",historyPrem + currentPrem));
            //大于0
            if (historyPrem > dou) {
                if(sum > dou ){
                    sumState.put(sales_id, sum);
                }else {
                    sumState.put(sales_id, sum);
                    if(countState.get(key_id) > 0) {
                        countState.put(key_id, countState.get(key_id) - 1);
                    }else {
                        countState.put(key_id, 0);
                    }
                }
                //小于等于0
            }else {
                if(sum > dou ){
                    sumState.put(sales_id, sum);
                    countState.put(key_id, countState.get(key_id) + 1);
                }else {
                    sumState.put(sales_id, sum);
                }
            }
        } else {
            //不存在sales_id
            //存在日期加机构
            if (countState.contains(key_id)) {
                Double ycc = Utils.str2DouIsNull(String.format("%.2f", Double.valueOf(getHbaseValue(hTableManpower_Tmp1Table,sales_id.split("#")[1], "f", sales_id.split("#")[1],"m_qj_manpower_prem")) + currentPrem));
                sumState.put(sales_id, ycc);
                if (ycc > dou) {
                    countState.put(key_id, countState.get(key_id) + 1);
                } else {
                    //小于等于0
                    countState.put(key_id, countState.get(key_id));
                }
                //不存在sales_id
                //不存在日期加机构
            } else {
                Double ycc = Utils.str2DouIsNull(String.format("%.2f", Double.valueOf(getHbaseValue(hTableManpower_Tmp1Table,sales_id.split("#")[1], "f", sales_id.split("#")[1],"m_qj_manpower_prem")) + currentPrem));
                sumState.put(sales_id, ycc);
                //大于0
                if (ycc > dou) {
                    countState.put(key_id, enter_order_monthManpower + 1);
                } else {
                    //小于等于0
                    countState.put(key_id, enter_order_monthManpower);
                }
            }
        }
    }

    /**
     * 当月合格人力
     * @param sumState
     * @param countState
     * @param key_id
     * @param sales_id
     * @param currentPrem
     * @param dou
     * @throws Exception
     */
    public static synchronized void monthQualifiedStateTotal(MapState<String, Double> sumState, MapState<String, Integer> countState, String key_id, String sales_id, Double currentPrem, HTable hTableManpower_Tmp1Table, Integer eligible_manpower_monthManpower, Double dou) throws Exception {
        //存在sales_id
        if (sumState.contains(sales_id)) {
            Double historyPrem = sumState.get(sales_id);
            double sum = Double.valueOf(String.format("%.2f",historyPrem + currentPrem));
            //大于等于3W
            if (historyPrem >= dou) {
                if(sum >= dou ){
                    sumState.put(sales_id, sum);
                }else {
                    sumState.put(sales_id, sum);
                    if(countState.get(key_id) > 0) {
                        countState.put(key_id, countState.get(key_id) - 1);
                    }else {
                        countState.put(key_id, 0);
                    }
                }
                //小于3W
            }else {
                if(sum >= dou ){
                    sumState.put(sales_id, sum);
                    countState.put(key_id, countState.get(key_id) + 1);
                }else {
                    sumState.put(sales_id, sum);
                }
            }
        } else {
            //不存在sales_id
            //存在日期加机构
            if (countState.contains(key_id)) {
                Double ycc = Utils.str2DouIsNull(String.format("%.2f", Double.valueOf(getHbaseValue(hTableManpower_Tmp1Table,sales_id.split("#")[1], "f", sales_id.split("#")[1],"m_qj_manpower_prem")) + currentPrem));
                sumState.put(sales_id, ycc);
                //大于等于3W
                if (ycc >= dou) {
                    countState.put(key_id, countState.get(key_id) + 1);
                } else {
                    //小于3W
                    countState.put(key_id, countState.get(key_id));
                }
                //不存在sales_id
                //不存在日期加机构
            } else {
                Double ycc = Utils.str2DouIsNull(String.format("%.2f", Double.valueOf(getHbaseValue(hTableManpower_Tmp1Table, sales_id.split("#")[1], "f", sales_id.split("#")[1],"m_qj_manpower_prem")) + currentPrem));
                sumState.put(sales_id, ycc);
                //大于等于3W
                if (ycc >= dou) {
                    countState.put(key_id, eligible_manpower_monthManpower + 1);
                } else {
                    //小于3W
                    countState.put(key_id, eligible_manpower_monthManpower);
                }
            }
        }
    }
}
