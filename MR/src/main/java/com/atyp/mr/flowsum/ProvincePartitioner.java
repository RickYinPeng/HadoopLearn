package com.atyp.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner<Text, FlowBean>
 * 这两个泛型时map阶段的输出
 */
public class  ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        // key 是手机号
        // value 是流量信息

        // 获取手机号前三位
        String prePhoneNum = key.toString().substring(0, 3);

        int partition = 4;

        if ("136".equals(prePhoneNum)) {
            partition = 0;
        } else if ("137".equals(prePhoneNum)) {
            partition = 1;
        } else if ("138".equals(prePhoneNum)) {
            partition = 2;
        } else if ("139".equals(prePhoneNum)) {
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}
