package com.atyp.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\input_05_fileinputFormat","D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\output_05_fileinputFormat"};

        Configuration conf = new Configuration();

        // 1.获取Job对象
        Job job = Job.getInstance(conf);

        // 2.设置Jar路径
        job.setJarByClass(SequenceFileDriver.class);

        // 3.关联mapper和reducer类
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 4.设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 7.设置输入的InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);

        // 8.设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 5.设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 6.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7.提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
