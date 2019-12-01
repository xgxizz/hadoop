package com.xu.hadoop.mapreduce.inputformat;

import com.xu.hadoop.mapreduce.wordcount.combiner.WordCountCombinerDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        //1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2 获取jar包位置
        job.setJarByClass(SequenceFileDriver.class);
        //3 指定本业务Job使用的mapper/reducer业务类
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);
        //4 指定Mapper输出数据的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        //5 指定最终输出数据的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //6 指定job的输入原始文件所在目录以及输出文件目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7 将job中配置的相关参数，以及job所用的Java类所在的jar包，提交给yarn去运行
        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
