package io.ddisk.hadoop.demo.controller;

import io.ddisk.hadoop.template.HadoopTemplate;
import io.ddisk.handler.nomal.PageDataHandler;
import io.ddisk.v1.SummaryVo;
import io.ddisk.vo.EntityResultVo;
import io.ddisk.vo.PagingDataVo;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import io.ddisk.hadoop.demo.reduce.WordCountMapper;
import io.ddisk.hadoop.demo.reduce.WordCountReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @Description
 * @author AbrahamVong
 * @since 2022/4/16
 */
@RestController
@RequestMapping(value = "/hadoop/demo")
public class DemoController {

    @Autowired
    private HadoopTemplate template;

    @Qualifier("hdfsConfig")
    @Autowired
    private org.apache.hadoop.conf.Configuration conf;

    /**
     * 将文件内容打印到控制台
     * @param name
     * @return
     */
    @GetMapping(value = "/sout")
    public String sout(@RequestParam("name") String name){
        template.readTo(template.getNameSpace(),"/"+name,4096);
        return "file content print in console";
    }

    /**
     * 读取文件内容返回String结果
     * @param name
     * @return
     */
    @GetMapping(value = "/read")
    public String read(@RequestParam("name") String name){
        return template.readContent(template.getNameSpace(),name);
    }

    /**
     * 写入或追加文件内容
     * @param name
     * @param content
     * @return
     */
    @GetMapping(value = "/write")
    public String weite(@RequestParam("name") String name,@RequestParam("content") String content){
        template.writeToHDFS(template.getNameSpace(),"/"+name,content);
        return "content has been written to the file";
    }

    /**
     * 添加本地文件到hdfs
     * @param name
     * @return
     */
    @GetMapping(value = "/add")
    public String add(@RequestParam("name") String name){
        template.copyFileToHDFS(true,false,name,template.getNameSpace());
        return "upload success";
    }

    /**
     * 移除hdfs的文件
     * @param name
     * @return
     */
    @GetMapping(value = "/rm")
    public String rm(@RequestParam("name") String name){
        template.rmdir("/garden","/" + name);
        return "remove success";
    }

    /**
     * 获取hdfs某dir下的文件列表
     * @param pageable
     * @return
     */
    @GetMapping(value = "/dir/list")
    public ResponseEntity<PagingDataVo> getDirList(@PageableDefault(page=1,size=5) Pageable pageable) {
        List<FileStatus> list = template.getDirList("/garden");
        Page<FileStatus> page = new PageImpl<FileStatus>(list,pageable,list.size());
        PagingDataVo paging = new PagingDataVo();
        PageDataHandler.wrapper(paging,page,FileStatus.class);
        return ResponseEntity.ok(paging);
    }

    /**
     * 获取服务器及hdfs的存储使用量信息
     * @return
     */
    @GetMapping(value = "/quota")
    public ResponseEntity<EntityResultVo<SummaryVo>> getQuota() {

        SummaryVo vo = new SummaryVo();
        ContentSummary quota = template.getRootQuota();
        BeanUtils.copyProperties(quota, vo);
        vo.setConsumed((double) vo.getSpaceConsumed() /(1024*1024));

        File win = new File("/");
        if (win.exists()) {
            long total = win.getTotalSpace();
            long freeSpace = win.getFreeSpace();
            vo.setMaxSpace(total/(1024*3));
            vo.setFreeSpace(freeSpace/(1024*3));
            vo.setUseSpace((total - freeSpace)/(1024*3));
        }

        return ResponseEntity.ok(new EntityResultVo<SummaryVo>().setEntity(vo));
    }

    /**
     * mapreduce字符统计
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @GetMapping(value = "/reduce")
    public Boolean reduce() throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(template.getNameSpace()));
        FileOutputFormat.setOutputPath(job, new Path("/statics"));

        return job.waitForCompletion(true);
    }

}
