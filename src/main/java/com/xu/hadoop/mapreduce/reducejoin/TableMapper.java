package com.xu.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private TableBean tableBean = new TableBean();
    private Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文件数据类型
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();

        //获取输入数据
        String line = value.toString();
        if (name.startsWith("order")){ //订单表
            String[] fields = line.split("\t");
            tableBean.setOrder_id(fields[0]);
            tableBean.setP_id(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setPname("");
            tableBean.setFlag("0");
            k.set(fields[1]);
        }else { //产品表
            String[] fields = line.split("\t");
            tableBean.setP_id(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("1");
            tableBean.setAmount(0);
            tableBean.setOrder_id("");
            k.set(fields[0]);
        }
        context.write(k, tableBean);
    }
}
