package com.atyp.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * FileInputFormat<Text, BytesWritable>
 *     Text: 读取的是整个文件，所以Text是文件的路径+文件名字
 *     BytesWritable: 文件的的内容
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(split,context);
        return recordReader;
    }
}
