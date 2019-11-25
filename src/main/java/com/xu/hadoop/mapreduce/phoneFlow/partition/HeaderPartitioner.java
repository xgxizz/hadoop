package com.xu.hadoop.mapreduce.phoneFlow.partition;

import com.xu.hadoop.mapreduce.phoneFlow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HeaderPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean o2, int i) {
        //获取手机号
        String phoneNum = key.toString();
        String header = phoneNum.substring(0,3);
        int partition = 4;
        if(header.equals("136")){
            partition = 0;
        } if(header.equals("137")){
            partition = 1;
        } if(header.equals("138")){
            partition = 2;
        } if(header.equals("139")){
            partition = 3;
        }
        return partition;
    }
}
