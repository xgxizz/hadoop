package com.xu.hadoop.mapreduce.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import static java.lang.Integer.MAX_VALUE;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/29 16:30
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    public int getPartition(OrderBean key, NullWritable nullWritable, int numReduceTasks) {
        return (key.getOrderNo() & MAX_VALUE ) % numReduceTasks;
    }
}
