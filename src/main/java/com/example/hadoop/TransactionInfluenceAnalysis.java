package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TransactionInfluenceAnalysis {

    // Mapper class: maps each record to a (Interest Rate Range, Purchase Amount)
    public static class InfluenceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // 假设文件按逗号分隔
            // String date = fields[0];
            double interestRate = Double.parseDouble(fields[1]); // 1周利率
            double purchaseAmount = Double.parseDouble(fields[4]); // 资金流入

            // 将利率分段，例如：0-2%, 2-4%, 4-6%, 6-8%...
            String interestRateRange = getInterestRateRange(interestRate);

            // 输出键值对: (利率区间, 资金流入)
            context.write(new Text(interestRateRange), new DoubleWritable(purchaseAmount));
        }

        // Helper function: 将利率划分为区间
        private String getInterestRateRange(double rate) {
            if (rate < 2.0) {
                return "0-2%";
            } else if (rate < 4.0) {
                return "2-4%";
            } else if (rate < 6.0) {
                return "4-6%";
            } else if (rate < 8.0) {
                return "6-8%";
            } else {
                return "8%+";
            }
        }
    }

    // Reducer class: calculates average purchase amount per interest rate range
    public static class InfluenceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalPurchase = 0;
            int count = 0;

            // 累加所有资金流入值并计算数量
            for (DoubleWritable val : values) {
                totalPurchase += val.get();
                count++;
            }

            // 计算该利率区间的平均资金流入
            double avgPurchase = count == 0 ? 0 : totalPurchase / count;
            context.write(key, new DoubleWritable(avgPurchase));
        }
    }

    // main方法：作业配置
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TransactionInfluenceAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Transaction Influence Analysis");

        // 设置主类
        job.setJarByClass(TransactionInfluenceAnalysis.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(InfluenceMapper.class);
        job.setReducerClass(InfluenceReducer.class);

        // 设置Map和Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // 输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
