package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortByActiveDays {

    // Mapper：交换 key-value，输出 <活跃天数, 用户ID>
    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();  // 去掉首尾空格
            if (line.isEmpty()) {
                return;  // 忽略空行
            }

            String[] fields = line.split("\\t");  // 使用 TAB 作为分隔符
            if (fields.length != 2) {
                System.err.println("Invalid input: " + line);  // 打印无效输入行日志
                return;  // 忽略格式不正确的行
            }

            try {
                String userId = fields[0];
                int activeDays = Integer.parseInt(fields[1]);
                context.write(new IntWritable(activeDays), new Text(userId));
            } catch (NumberFormatException e) {
                System.err.println("Invalid number format: " + fields[1]);  // 打印错误日志
            }
        }
    }

    // Reducer：输出 <用户ID, 活跃天数>
    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, key);  // 输出 <用户ID, 活跃天数>
            }
        }
    }

    // 自定义比较器：实现活跃天数的降序排列
    public static class DescendingComparator extends WritableComparator {
        protected DescendingComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable v1 = (IntWritable) a;
            IntWritable v2 = (IntWritable) b;
            return -1 * v1.compareTo(v2);  // 实现降序排列
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort By Active Days");
    
        job.setJarByClass(SortByActiveDays.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
    
        // Mapper 的输出类型（中间结果类型）
        job.setMapOutputKeyClass(IntWritable.class);  // 活跃天数
        job.setMapOutputValueClass(Text.class);       // 用户ID
    
        // 最终输出类型（Reducer 的输出类型）
        job.setOutputKeyClass(Text.class);            // 用户ID
        job.setOutputValueClass(IntWritable.class);   // 活跃天数
    
        // 设置自定义的降序比较器
        job.setSortComparatorClass(DescendingComparator.class);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
