package com.xu.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    //hello 1
    //hello 1

    //world 1
    //world 1
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        // 1. 统计单词个数
        int sum = 0;
        for (LongWritable count:values) {
            sum += count.get();
        }
        // 2. 输出单词个数
        context.write(key, new LongWritable(sum));
    }
}
