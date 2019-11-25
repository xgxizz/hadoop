package com.xu.hadoop.mapreduce.phoneFlow.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneFlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phoneNum = fields[0];
        k.setUp(Double.parseDouble(fields[fields.length-3]));
        k.setDown(Double.parseDouble(fields[fields.length-2]));
        k.setSum(k.getUp() + k.getDown());
        v.set(phoneNum);
        context.write(k,v);
    }
}
