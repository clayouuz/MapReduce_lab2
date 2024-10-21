package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TransactionInfluenceAnalysis {

    // 枚举计数器类型，用于全局计数
    public static enum InfluenceCounter {
        TOTAL_ONES // 用于统计全局1的数量
    }

    public static class YieldMapper extends Mapper<Object, Text, Text, IntWritable> {

        // 定义常量1和0，用于表示lambda > y 或 lambda < y
        private final static IntWritable ONE = new IntWritable(1);
        private final static IntWritable ZERO = new IntWritable(0);
        private Text date = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            // 忽略表头
            if (line.startsWith("mfd_date")) {
                return;
            }

            // 使用逗号分隔符解析每行数据，去除空格
            String[] fields = line.split(",\\s*");
            if (fields.length == 3) {
                String mfdDate = fields[0]; // mfd_date 列
                double mfdDailyYield = Double.parseDouble(fields[1]); // mfd_daily_yield 列
                double mfdWeeklyYield = Double.parseDouble(fields[2]);
                // 计算 lambda
                double lambda = 100 * (Math.pow((mfdDailyYield / 10000 + 1), 365) - 1);

                // 比较 lambda 和 mfd_daily_yield，输出1或0
                date.set(mfdDate); // 使用日期作为key
                if (lambda > mfdWeeklyYield) {
                    context.write(date, ONE); // lambda > y 输出 1
                } else {
                    context.write(date, ZERO); // lambda < y 输出 0
                }
            }
        }
    }

    public static class YieldReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int globalOneCount = 0; // 全局1的计数器

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(key, val); // 输出日期和1或0
                globalOneCount += val.get(); // 更新全局1的计数器
            }            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 在任务结束时输出全局计数器的值
            String globalResult = "当日年化收益率小于本周年化收益率: " + globalOneCount + "\n";
            context.write(new Text(globalResult), null); // 输出全局1的数量
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Transaction Influence Analysis");
        job.setJarByClass(TransactionInfluenceAnalysis.class);

        // 设置 Mapper 和 Reducer
        job.setMapperClass(YieldMapper.class);
        job.setReducerClass(YieldReducer.class);

        // 设置输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

        // 提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
