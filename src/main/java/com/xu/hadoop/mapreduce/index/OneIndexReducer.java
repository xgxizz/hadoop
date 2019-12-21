package com.xu.hadoop.mapreduce.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneIndexReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private Text outputKey = new Text();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String k = key.toString();
        String[] splits = k.split("--");
        String filename = splits[1];
        int sum = 0;
        for (IntWritable num: values) {
            sum ++;
        }
        outputKey.set(splits[0] + "\t" +filename + "---->" + sum);
        context.write(outputKey, NullWritable.get());
    }
}
