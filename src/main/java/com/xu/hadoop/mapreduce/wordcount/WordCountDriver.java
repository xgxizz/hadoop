package com.xu.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

//https://blog.csdn.net/congcong68/article/details/42043093/
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        //开启map端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress", true);
        //设置map端输出压缩方式
        /**
         * mapreduce.map.output.compress.codec:KEY
         * BZip2Codec.class  VALUE
         * CompressionCodec.class 实现的接口
         */
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);
        //2 获取jar包位置
        job.setJarByClass(WordCountDriver.class);
        //3 指定本业务Job使用的mapper/reducer业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4 指定Mapper输出数据的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //5 指定最终输出数据的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //6 指定job的输入原始文件所在目录以及输出文件目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        //设置压缩方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        //7 将job中配置的相关参数，以及job所用的Java类所在的jar包，提交给yarn去运行
        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
