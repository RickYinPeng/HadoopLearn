package com.atyp.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosatguigu;
    FSDataOutputStream fosother;

    public FRecordWriter(TaskAttemptContext job) {
        try {
            // 1.获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());

            // 2.创建输出到atguigu.log的输出流
            fosatguigu = fs.create(new Path("D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\atguigu.log"));

            // 3.创建输出到other.log的输出流
            fosother = fs.create(new Path("D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断 key 当中是否有 atguigu ，如果有写出到 atguigu.log ，如果没有写出到 other.log
        if (key.toString().contains("atguigu")) {
            // atguigu输出流
            fosatguigu.write(key.toString().getBytes());
        } else {
            fosother.write(key.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fosatguigu);
        IOUtils.closeStream(fosother);
    }
}
