package com.xu.hadoop.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DistributedMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读取缓存文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("e:/tmp/inputCache/pd.txt"),"UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            //切割
            String[] fields = line.split("\t");
            pdMap.put(fields[0], fields[1]);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        //获取pid
        String pid = fields[1];
        //根据pid获取pdMap中的产品名称
        String pdName = pdMap.get(pid);
        //拼接
        line = line + "\t" + pdName;
        //输出
        k.set(line);
        context.write(k, NullWritable.get());
    }
}
