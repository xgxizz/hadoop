package com.xu.hadoop.mapreduce.phoneFlow.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//ʵ�ַ����ӿڣ�����ͨ������ʵ�ַ�����������
public class PhoneSortPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean key, Text value, int i) {

        int patition = 4;
        String phoneNum = value.toString();
        if (phoneNum.startsWith("136")){
            patition = 0;
        }
        if (phoneNum.startsWith("137")){
            patition = 1;
        }
        if (phoneNum.startsWith("138")){
            patition = 2;
        }
        if (phoneNum.startsWith("139")){
            patition = 3;
        }
        return patition;
    }
}
