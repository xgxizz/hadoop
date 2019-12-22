package com.xu.hadoop.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //压缩
        compress("e:/tmp/hello.txt","org.apache.hadoop.io.compress.BZip2Codec");
//        compress("e:/tmp/hello.txt","org.apache.hadoop.io.compress.GzipCodec");
//        compress("e:/tmp/hello.txt","org.apache.hadoop.io.compress.DefaultCodec");
        //解压缩
        deCompress("e:/tmp/hello.txt.deflate");
    }

    /**
     * 压缩文件
     * @param filename 待压缩文件名
     * @param method 压缩方式
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static void compress(String filename, String method) throws IOException, ClassNotFoundException {

        //1 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));

        Class<?> className = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(className, new Configuration());
        //2 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        //对拷
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);
        //关闭流
        fis.close();
        cos.close();
        fos.close();
    }

    /**
     * 解压缩
     * @param filename 待解压文件
     */
    private static void deCompress(String filename) throws IOException {
        //0 校验 看CompressFactory中是否有对应的解码方式
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null){
            System.out.println("cannot find codec for file " + filename);
            return;
        }
        //1 获取输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(filename));
        //2 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + ".decode"));
        //3 流对拷
        IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5, false);

        cis.close();
        fos.close();
    }
}
