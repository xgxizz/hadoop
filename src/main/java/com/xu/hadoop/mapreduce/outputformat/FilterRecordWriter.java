package com.xu.hadoop.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream baiduDataOutputStream;
    private FSDataOutputStream otherDataOutputStream;
    public FilterRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        Configuration configuration = taskAttemptContext.getConfiguration();
        Job job = Job.getInstance(configuration);
        FileSystem fs = FileSystem.get(configuration);

        //创建文件输出路径
        Path baiduPath = new Path("e:/tmp/outputformat/output/baidu.log");
        Path otherPath = new Path("e:/tmp/outputformat/output/other.log");

        //创建输出流
        this.baiduDataOutputStream = fs.create(baiduPath, true);
        this.otherDataOutputStream = fs.create(otherPath, true);
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String s = text.toString();
        if (s.contains("baidu")){
            baiduDataOutputStream.write(s.getBytes());
        }else {
            otherDataOutputStream.write(s.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (baiduDataOutputStream!= null){
            baiduDataOutputStream.close();
        }
        if (otherDataOutputStream != null){
            otherDataOutputStream.close();
        }
    }
}
