package com.xu.hadoop.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/29 16:43
 */
public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        if (aBean.getOrderNo() > bBean.getOrderNo()){
            return 1;
        }else if (aBean.getOrderNo() < bBean.getOrderNo()){
            return -1;
        }else {
            return 0;
        }
    }
}
