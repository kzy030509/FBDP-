package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: Driver <input path><training data><test data> <output path1> <output path2> <output path3>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();

        // 任务一 - 统计违约和非违约的数量
        Job job1 = Job.getInstance(conf, "Default Count");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(DefaultCountMapper.class);
        job1.setReducerClass(DefaultCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        
        // 任务二 - 统计每天申请贷款的交易数
        Job job2 = Job.getInstance(conf, "Daily Loan Count");
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(DailyLoanCountMapper.class);
        job2.setReducerClass(DailyLoanCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        
        // 任务三 - 基于贷款数据建立违约检测模型

        conf.setInt("K",5);//设置KNN算法的K值
        conf.setInt("testDataNum",0);//设置全局计数器，记录测试数据数目
        conf.setInt("dimension",8);//设置向量维度
        conf.setInt("TP",0);
        conf.setInt("TN",0);
        conf.setInt("FP",0);
        conf.setInt("FN",0);
        Job job3 = Job.getInstance(conf, "Default Detection");
        job3.setJarByClass(Driver.class);
        job3.setMapperClass(KNN_Mapper.class);
        job3.setReducerClass(KNN_Reducer.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Text.class);
        job3.addCacheFile(new Path(args[1]).toUri());
        
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[5]));
        // 提交作业并等待完成
        boolean job1Completed = job1.waitForCompletion(true);
        boolean job2Completed = job2.waitForCompletion(true);
        boolean job3Completed = job3.waitForCompletion(true);

                      
        System.exit(job1Completed && job2Completed && job3Completed ? 0 : 1);
    }
}