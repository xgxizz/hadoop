package com.xu.hadoop.mapreduce.phoneFlow.partition;

import com.xu.hadoop.mapreduce.phoneFlow.FlowBean;
import com.xu.hadoop.mapreduce.phoneFlow.PhoneFlowMapper;
import com.xu.hadoop.mapreduce.phoneFlow.PhoneFlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/25 16:22
 */
public class PhoneFlowWithPartitionDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(PhoneFlowWithPartitionDriver.class);

        job.setMapperClass(PhoneFlowMapper.class);
        job.setReducerClass(PhoneFlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //…Ë÷√∑÷«¯
        job.setPartitionerClass(HeaderPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);


    }
}
