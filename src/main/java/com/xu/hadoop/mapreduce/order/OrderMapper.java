package com.xu.hadoop.mapreduce.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/29 14:40
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean k = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String orderNo = split[0];
        String price = split[2];
        k.setOrderNo(Integer.parseInt(orderNo));
        k.setPrice(Double.parseDouble(price));
        context.write(k, NullWritable.get());
    }
}
