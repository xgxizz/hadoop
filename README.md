# hadoop
基于Java的Hadoop核心功能实现。包括HDFS及MapReduce等。

#1.项目描述：
    该项目分为两个模块，分别是hdfs模块和mapreduce模块。
#1.1 hdfs模块
    主要包括Java操作HDFS，如获取HDFS文件系统，读取文件信息，上传下载文件等。其中上传下载文件包含“流式操作”和“HDFS的API操作”两种方式。
#1.2 mapReduce模块
    一些mapReduce的实例。
#1.2.1 wordCount(单词统计) 位置：com.xu.hadoop.mapreduce.wordcount,包含默认的统计方式，同时还有combiner 和 combineText 的实现。
#1.2.2 phoneFlow（流量统计）位置：com.xu.hadoop.mapreduce.phoneFlow，实现了分区输出 以及 排序。
    
