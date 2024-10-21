package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.hadoop.DailyFlowStats;
import com.example.hadoop.TransactionInfluenceAnalysis;
import com.example.hadoop.UserActivityAnalysis;
import com.example.hadoop.WeeklyFlowStats;
import com.example.hadoop.SortByActiveDays;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Main <task> <input path> <output path>");
            System.exit(-1);
        }

        String task = args[0];  // 第一个参数决定任务类型
        String inputPath = args[1];  // 第二个参数为输入路径
        String outputPath = args[2]; // 第三个参数为输出路径

        Configuration conf = new Configuration();
        Job job;

        switch (task) {
            case "dailyFlow":  // 任务1：每日资金流入流出统计
                job = Job.getInstance(conf, "Daily Flow Stats");
                job.setJarByClass(DailyFlowStats.class);
                job.setMapperClass(DailyFlowStats.FlowMapper.class);
                job.setReducerClass(DailyFlowStats.FlowReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;

            case "weeklyFlow":  // 任务2：星期交易量统计
                job = Job.getInstance(conf, "Weekly Flow Stats");
                job.setJarByClass(WeeklyFlowStats.class);
                job.setMapperClass(WeeklyFlowStats.WeekdayMapper.class);
                job.setReducerClass(WeeklyFlowStats.WeekdayReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;

            case "userActivity":  // 任务3：用户活跃度分析
                job = Job.getInstance(conf, "User Activity Analysis");
                job.setJarByClass(UserActivityAnalysis.class);
                job.setMapperClass(UserActivityAnalysis.ActivityMapper.class);
                job.setReducerClass(UserActivityAnalysis.ActivityReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                
                break;
                case "sortByActivateDays":
                job = Job.getInstance(conf, "Sort By Activate Days");
                job.setJarByClass(SortByActiveDays.class);
            
                // 设置 Mapper 和 Reducer 类
                job.setMapperClass(SortByActiveDays.SortMapper.class);
                job.setReducerClass(SortByActiveDays.SortReducer.class);
            
                // 设置 Mapper 的输出类型（中间结果类型）
                job.setMapOutputKeyClass(IntWritable.class);  // 活跃天数
                job.setMapOutputValueClass(Text.class);       // 用户ID
            
                // 设置 Reducer 的最终输出类型
                job.setOutputKeyClass(Text.class);            // 用户ID
                job.setOutputValueClass(IntWritable.class);   // 活跃天数
            
                // 设置自定义降序比较器
                job.setSortComparatorClass(SortByActiveDays.DescendingComparator.class);
                break;
            
            case "transactionInfluence":  // 任务4：交易行为影响因素分析
                job = Job.getInstance(conf, "Transaction Influence Analysis");
                job.setJarByClass(TransactionInfluenceAnalysis.class);
                job.setMapperClass(TransactionInfluenceAnalysis.InfluenceMapper.class);
                job.setReducerClass(TransactionInfluenceAnalysis.InfluenceReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                break;

            default:
                System.err.println("Invalid task type. Available tasks: dailyFlow, weeklyFlow, userActivity, transactionInfluence");
                System.exit(-1);
                return;
        }

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 提交作业并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
