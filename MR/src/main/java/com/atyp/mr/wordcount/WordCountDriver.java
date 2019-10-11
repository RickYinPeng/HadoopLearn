package com.atyp.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\input_01","D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\output_01"};

        Configuration conf = new Configuration();
        // 1.获取Job对象
        Job job = Job.getInstance(conf);

        // 2.设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

        // 3.关联Map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置Mapper阶段数据输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        //job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置为 4M
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        //如果 NumReduceTasks 的个数你不设置，那默认是1(任何数%1=0)，那么分区就没有任何意义
        //job.setNumReduceTasks(2);

        // 指定需要使用 combiner ，以及用哪个类作为 combiner 的逻辑
        job.setCombinerClass(WordCountCombiner.class);

        //关联合并类
        //job.setCombinerClass(WordCountReducer.class);

        // 6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7.提交Job
        //job.submit();
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
