package com.xu.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入的Key   LongWritable   行号
 * 输入的value Test           一行内容
 * 输出的Key   Text           单词
 * 输出的value LongWritable   单词个数
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text k = new Text();
    private LongWritable v = new LongWritable(1);
    /**
     *
     hello world
     xgx xgx
     hadoop
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        //1. 一行转换成String
        String line = value.toString();
        //2. 切割
        String []words = line.split(" ");
        //3. 循环写出到下一个阶段
        for (String word:words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
