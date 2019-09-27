package com.atyp.mr.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NLineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\input_04_nline","D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\output_04_nline"};

        Configuration conf = new Configuration();

        // 1.获取Job对象
        Job job = Job.getInstance(conf);

        // 8.设置每个切片 InputSplit 中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job,3);

        // 9.使用 NLineInputFormat 处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        // 2.设置Jar路径
        job.setJarByClass(NLineDriver.class);

        // 3.关联mapper和reducer类
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        // 4.设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7.提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
