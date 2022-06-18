package io.ddisk.hadoop.template;

import io.ddisk.hadoop.hdfs.dto.FileInfoDto;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Description
 * @author AbrahamVong
 * @since 2022/4/16
 */
@Service
@ConditionalOnBean(FileSystem.class)
public class HadoopTemplate {

    @Autowired
    private FileSystem fileSystem;
    private String nameSpace="/garden";

    @PostConstruct
    public void init(){
        existDir(nameSpace,true);
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void uploadFile(String srcFile){
        copyFileToHDFS(false,true,srcFile,nameSpace);
    }

    public void uploadFile(String srcFile,String destPath){
        copyFileToHDFS(false,true,srcFile,destPath);
    }

    public void delFile(String fileName){
        rmdir(nameSpace,fileName) ;
    }

    public void delDir(String path){
        nameSpace = nameSpace + "/" +path;
        rmdir(path,null) ;
    }

    public void download(String fileName,String savePath){
        getFile(nameSpace+"/"+fileName,savePath);
    }


    /**
     * 获取根目录的储存使用量
     * @return
     */
    @SneakyThrows
    public ContentSummary getRootQuota(){
        return fileSystem.getContentSummary(new Path( "/"));
    }

    /**
     * 获取当前目录列表的所有文件信息
     * @param path
     * @return
     */
    public List<FileStatus> getDirList(String path){

        List<FileStatus> list = new ArrayList<>();
        try {

            FileStatus[] statuses = fileSystem.listStatus(new Path(path));
            list.addAll(Arrays.stream(statuses).collect(Collectors.toList()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public boolean existFile(String filePath, String fileName){
        boolean flag = false;
        String position =  filePath + "/" +fileName;
        if(StringUtils.isEmpty(filePath)){
            throw new IllegalArgumentException("filePath不能为空");
        }
        try{
            Path path = new Path(position);
            if (fileSystem.isFile(path) | fileSystem.isDirectory(path)){
                flag = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 创建目录
     * @param filePath
     * @param create
     * @return
     */
    public boolean existDir(String filePath, boolean create){
        boolean flag = false;
        if(StringUtils.isEmpty(filePath)){
            throw new IllegalArgumentException("filePath不能为空");
        }
        try{
            Path path = new Path(filePath);
            if (create){
                if (!fileSystem.exists(path)){
                    fileSystem.mkdirs(path);
                }
            }
            if (fileSystem.isDirectory(path)){
                flag = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 文件上传至 HDFS
     * @param delSrc       指是否删除源文件，true为删除，默认为false
     * @param overwrite
     * @param srcFile      源文件，上传文件路径
     * @param destPath     hdfs的目的路径
     */
    public  void copyFileToHDFS(boolean delSrc, boolean overwrite,String srcFile,String destPath) {
        // 源文件路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/weibo.txt
        Path srcPath = new Path(srcFile);

        // 目的路径
        Path dstPath = new Path(destPath);
        // 实现文件上传
        try {
            // 获取FileSystem对象
            fileSystem.copyFromLocalFile(srcPath, dstPath);

            //fileSystem.copyFromLocalFile(delSrc,overwrite,srcPath, dstPath);
            //释放资源
            //    fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件读取到控制台输出
     * @param path 目录
     * @param fileName 文件名
     * @param size 读取大小
     */
    public  void readTo(String path,String fileName,int size) {
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = fileSystem.open(new Path(path + "/" +fileName));
            IOUtils.copyBytes(fsDataInputStream, System.out, size, false);
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(fsDataInputStream!=null){
                    fsDataInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 文件内容的读取
     * @param path 目录
     * @param fileName 文件名
     */
    public String readContent(String path,String fileName) {

        HashSet<String> rowSet = new HashSet<>();
        StringBuilder content = new StringBuilder();
        FSDataInputStream fin = null;
        BufferedReader in = null;
        String line;
        try {
            fin = fileSystem.open(new Path(path + "/" +fileName));
            in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));

            while ((line = in.readLine()) != null) {
                rowSet.add(line + "\n");
            }
//            System.out.println(rowSet.size());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(fin!=null){
                    fin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        for(String row : rowSet){
            content.append(row);
        }

        return content.toString();
    }

    /**
     * 文件内容的写入
     * @param path 目录
     * @param fileName 文件名
     * @param content 追加的内容
     */
    public void writeToHDFS(String path,String fileName,String content) {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            Path hdfsPath = new Path(path + "/" +fileName);
            if (!fileSystem.exists(hdfsPath)) {
                fsDataOutputStream = fileSystem.create(hdfsPath,false);
                fsDataOutputStream.writeUTF(content);
            }else{
                fsDataOutputStream = fileSystem.append(hdfsPath);
                fsDataOutputStream.writeUTF("\n"+content);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(fsDataOutputStream!=null){
                    fsDataOutputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 删除文件或者文件目录
     *
     * @param path
     */
    public void rmdir(String path,String fileName) {
        try {
            // 返回FileSystem对象
            if(StringUtils.isNotBlank(fileName)){
                path =  path + "/" +fileName;
            }
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            fileSystem.delete(new Path(path),true);
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从 HDFS 下载文件
     *
     * @param hdfsFile
     * @param destPath 文件下载后,存放地址
     */
    public void getFile(String hdfsFile,String destPath) {
        // 源文件路径
        Path hdfsPath = new Path(hdfsFile);
        Path dstPath = new Path(destPath);
        try {
            // 下载hdfs上的文件
            fileSystem.copyToLocalFile(hdfsPath, dstPath);
            // 释放资源
            // fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
