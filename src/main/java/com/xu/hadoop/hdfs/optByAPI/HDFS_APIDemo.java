package com.xu.hadoop.hdfs.optByAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFS_APIDemo {


    //获取文件系统
    @Test
    public void initHDFS() throws IOException {
        //1. 创建配置信息对象
        Configuration conf = new Configuration();
        //2. 获取文件系统
        FileSystem fs = FileSystem.get(conf);
        //3. 打印文件系统
        System.out.println(fs.toString());
        //4. 关闭文件系统
        fs.close();
    }

    //上传文件（测试优先级）
    // 客户端代码（配置文件）优先级最高
    @Test
    public void testCopyFromLocal() throws URISyntaxException, IOException, InterruptedException {
        //1. 获取文件系统
        Configuration conf = new Configuration();
        conf.set("dfs.replication","2");//HDFS系统中的副本数是3，治理设置成2
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //2. 上传文件
        fs.copyFromLocalFile(new Path("E:\\tmp\\hello.txt"), new Path("/usr/xgx/input/hello22.txt"));
        //3. 关闭资源
        fs.close();
        System.out.println("finished!!");
    }

    //文件下载
    @Test
    public void copyFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1. 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //2. 下载文件
        fs.copyToLocalFile(false, new Path("/usr/xgx/input/hello.txt"), new Path("E:\\tmp\\hello_download.txt"), true);
        //3. 关闭资源
        fs.close();
        System.out.println("下载文件完成！！");
    }

    //创建目录
    @Test
    public void mkdir() throws URISyntaxException, IOException, InterruptedException {
        //获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //创建文件夹
        fs.mkdirs(new Path("/usr/xgx/output"));
        fs.close();
        System.out.println("创建文件夹成功！");
    }
    //创建目录
    @Test
    public void delDir() throws URISyntaxException, IOException, InterruptedException {
        //获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //创建文件夹
        fs.delete(new Path("/usr/xgx/input/output/"),true);
        fs.close();
        System.out.println("删除文件夹成功！");
    }
    //文件更改名称
    @Test
    public void renameFile() throws URISyntaxException, IOException, InterruptedException {
        //获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        //创建文件夹
        fs.rename(new Path("/usr/xgx/input/jinlian.txt"),new Path("/usr/xgx/input/Jinlian.txt"));
        fs.close();
        System.out.println("文件名陈更改成功！");
    }

    //文件详情查看
    //文件名称，权限，长度，块信息
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        //获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus status = locatedFileStatusRemoteIterator.next();

            //输出详情
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            //长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            //z 组
            System.out.println(status.getGroup());

            //获取块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.print("所在主机：");
                for (String host:hosts) {
                    System.out.print(host + " ");
                }
                System.out.println("\n\n");
            }
        }
        fs.close();
    }

    //判断是文件还是文件夹
    @Test
    public void judgeFileOrDir() throws URISyntaxException, IOException, InterruptedException {
        //1. 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "xgx");
        FileStatus[] fileStatuses = fs.listStatus(new Path("/usr/"));
        for (FileStatus status: fileStatuses) {
            if (status.isDirectory()){
                System.out.println(status.getPath().getName() + " is directory.");
            }else{
                System.out.println(status.getPath().getName() + "is file.");
            }
        }
        fs.close();

    }
}
