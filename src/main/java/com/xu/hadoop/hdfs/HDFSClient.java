package com.xu.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        //1. 获取文件系统
        Configuration conf = new Configuration();
        //配置在集群上运行
//        conf.set("fs.defaultFS","hdfs://hadoop102:9000");
//        FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf, "xgx" );
        //上传文件
        fs.copyFromLocalFile(new Path("E:\\tmp\\jinlian.txt"), new Path("/usr/xgx/input/zaiyiqi.txt"));
        //关闭资源
        fs.close();

        System.out.println("finished!!");
    }
}
