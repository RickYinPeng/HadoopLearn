package com.atyp.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer<Text, IntWritable, Text, IntWritable>
 * 此合并案例中Reucer的参数是：前两个是map阶段的输出参数，后两个是最终要输出的参数类型
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1.累加求和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        v.set(sum);

        // 2.写出
        context.write(key, v);
    }
}
