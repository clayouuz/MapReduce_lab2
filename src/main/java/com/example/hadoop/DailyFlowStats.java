package com.example.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DailyFlowStats {

    public static class FlowMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String date = fields[1]; // 交易日期
            String purchaseAmt = fields[4].isEmpty() ? "0" : fields[4]; // 资金流入
            String redeemAmt = fields[9].isEmpty() ? "0" : fields[9]; // 资金流出
            context.write(new Text(date), new Text(purchaseAmt + "," + redeemAmt));
        }
    }

    public static class FlowReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalPurchase = 0;
            long totalRedeem = 0;
            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                totalPurchase += Long.parseLong(amounts[0]);
                totalRedeem += Long.parseLong(amounts[1]);
            }
            context.write(key, new Text(totalPurchase + "," + totalRedeem));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Daily Flow Stats");
        job.setJarByClass(DailyFlowStats.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
