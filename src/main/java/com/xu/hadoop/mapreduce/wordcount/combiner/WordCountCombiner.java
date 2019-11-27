package com.xu.hadoop.mapreduce.wordcount.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/27 9:53
 */
public class WordCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (LongWritable value :values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
