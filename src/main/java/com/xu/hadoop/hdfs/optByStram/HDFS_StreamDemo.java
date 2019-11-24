package com.xu.hadoop.hdfs.optByStram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 通过流的方式操作HDFS
 */
public class HDFS_StreamDemo {


    //文件上传
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //2 获取输入流
        FileInputStream is = new FileInputStream(new File("E:\\tmp\\zaiyiqi.txt"));
        //3 获取输出流
        FSDataOutputStream os = fs.create(new Path("/usr/xgx/zaiyiqi.txt"),true);
        //4 流对拷
        IOUtils.copyBytes(is, os, conf);
        //关闭资源
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);
        fs.close();
        System.out.println("上传完成");
    }

    @Test
    //文件下载
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //2 获取输入流
        FSDataInputStream is = fs.open(new Path("/usr/xgx/input/Jinlian.txt"));
        //3 获取输出流
        FileOutputStream os = new FileOutputStream(new File("E:\\tmp\\jinlian_stream_download.txt"));
        //4 流对拷
        IOUtils.copyBytes(is, os, conf);
        // 关闭资源
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);
        fs.close();
        System.out.println("下载完成");
    }
    //定位文件读取
    //读取第一块上的数据
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        //1. 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //2. 获取输入流
        FSDataInputStream is = fs.open(new Path("/usr/xgx/input/hadoop-2.7.2.tar.gz"));
        byte []buff = new byte[1024];
        FileOutputStream os = new FileOutputStream("E:\\tmp\\hadoop-2.7.2.tar.gz.part1");

        for (int i = 0; i < 128 *1024; i++) {
            is.read(buff);
            os.write(buff);
        }
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);
        fs.close();
        System.out.println("第一部分流式下载完成");
    }

    @Test
    //读取第二块上的数据
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        //1. 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //2. 获取输入流
        FSDataInputStream is = fs.open(new Path("/usr/xgx/input/hadoop-2.7.2.tar.gz"));
        //3. 定位输入数据位置
        is.seek(1024*1024*128);//1kb=1024bytes=1024*8bit
        //4. 获取输出流
        FileOutputStream os = new FileOutputStream("E:\\tmp\\hadoop-2.7.2.tar.gz.part2");
        //5. 流的对拷
        IOUtils.copyBytes(is, os, conf);
        //6. 关闭资源
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);
        fs.close();
        System.out.println("第二部分流式下载完成");

    }

}
