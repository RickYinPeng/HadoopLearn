package com.atyp.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    // 需要写一个构造
    public OrderGroupingComparator() {
        // 第二个参数为true，如果不是true，就会报空指针异常，可以看源码
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 要求只要id相同，就认为是相同的key
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int result;
        if (aBean.getOrder_id() > bBean.getOrder_id()) {
            result = 1;
        } else if (aBean.getOrder_id() < bBean.getOrder_id()) {
            result = -1;
        } else {
            // 如果是id相等，那么就会将相等的数据传入同一个reduce中
            result = 0;
        }
        return result;
    }
}
