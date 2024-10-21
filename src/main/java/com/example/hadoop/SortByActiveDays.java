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

    // Mapper：交换 key-value，输出 <活跃天数, userId>
    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            String userId = fields[0];
            int activeDays = Integer.parseInt(fields[1]);
            context.write(new IntWritable(activeDays), new Text(userId));
        }
    }

    // Reducer：直接输出 <活跃天数, userId>
    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, key);  // 输出 <userId, 活跃天数>
            }
        }
    }

    // 自定义比较器：实现活跃天数降序排列
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

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置自定义降序比较器
        job.setSortComparatorClass(DescendingComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
