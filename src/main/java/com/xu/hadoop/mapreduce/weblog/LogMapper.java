package com.xu.hadoop.mapreduce.weblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行
        String line = value.toString();
        boolean result = parseLog(line, context);
        if (!result){
            return;
        }
        k.set(line);
        context.write(k, NullWritable.get());
    }

    //解析line,清除脏数据
    private boolean parseLog(String line, Context context) {
        //切割
        String fields[] = line.split(" ");
        if (fields.length > 11){
            return true;
        }else {
            return false;
        }
    }
}
