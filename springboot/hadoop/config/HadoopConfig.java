package io.ddisk.hadoop.config;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

/**
 * @Description
 * @author AbrahamVong
 * @since 2022/4/16
 */
@Configuration
public class HadoopConfig {

    @Bean("hdfsConfig")
    public org.apache.hadoop.conf.Configuration hdfsChannel(){
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("mapred.job.tracker", "hdfs://180.76.178.1:8020/");
        conf.set("fs.defaultFS", "hdfs://180.76.178.1:8020/");
        System.setProperty("HADOOP_USER_NAME","root");
        return conf;
    }

    @Bean("fileSystem")
    public FileSystem createFs(@Qualifier("hdfsConfig") org.apache.hadoop.conf.Configuration conf){
        FileSystem fs = null;
        try {
            URI uri = new URI("hdfs://180.76.178.1:8020/");
            fs = FileSystem.get(uri,conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  fs;
    }

}
