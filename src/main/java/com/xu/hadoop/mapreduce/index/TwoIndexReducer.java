package com.xu.hadoop.mapreduce.index;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text, Text, Text, NullWritable> {

    private Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(key);
        for (Text v : values) {
            stringBuilder.append("\t" + v.toString());
        }
        k.set(stringBuilder.toString());
        context.write(k, NullWritable.get());
    }
}
