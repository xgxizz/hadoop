package com.xu.hadoop.mapreduce.phoneFlow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/25 15:46
 */
public class PhoneFlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phoneNum = split[0];
        double up = Double.parseDouble(split[split.length - 2]);
        double down = Double.parseDouble(split[split.length - 3]);
        k.set(phoneNum);
        v.setUp(up);
        v.setDown(down);
        v.setSum(up + down);
        context.write(k, v);
    }
}
