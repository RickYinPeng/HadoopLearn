package com.atyp.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Reducer阶段
// Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
// KEYIN,VALUEIN    map阶段输出的key和value(注意：是manp阶段的输出的k和v)
// KEYOUT,VALUEOUT  输出的格式是 atyp,2 所以是 Text, IntWritable
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // atyp,1
        // atyp,1

        int sum = 0;
        // 1.累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }

        v.set(sum);

        // 2.写出 atyp 2
        context.write(key,v);
    }
}
