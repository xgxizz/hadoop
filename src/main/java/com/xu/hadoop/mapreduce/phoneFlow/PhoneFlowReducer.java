package com.xu.hadoop.mapreduce.phoneFlow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/25 16:12
 */
public class PhoneFlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        while (values.iterator().hasNext()){
            FlowBean next = values.iterator().next();
            next.setSum(next.getUp() + next.getDown());
            context.write(key, next);
        }
    }
}
