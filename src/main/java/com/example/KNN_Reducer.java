package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class KNN_Reducer extends Reducer<LongWritable, Text,LongWritable, Text> {
    private Configuration conf;//全局配置
    private LongWritable longWritable = new LongWritable();
    private LongWritable TP= new LongWritable();
    private LongWritable FP= new LongWritable();
    private LongWritable TN= new LongWritable();
    private LongWritable FN= new LongWritable();
    public void reduce(LongWritable key, Text values,
                       Context context
    ) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        longWritable.set(conf.getLong("testDataNum",0));
        TP.set(conf.getLong("TP",0));
        FP.set(conf.getLong("FP",0));
        TN.set(conf.getLong("TN",0));
        FN.set(conf.getLong("FN",0));
        float tp = (float) TP.get();
        float tn = (float) TN.get();
        float fp = (float) FP.get();
        float fn = (float) FN.get();
        float total = (float) longWritable.get();
        // 输出评估结果
        float accuracy = (tp+tn)/total;
        float precision = tp/(tp+fp);
        float recall = tp/(tp+fn);
        float f1 = 2*precision*recall/(precision+recall);
        System.out.println("Total: " + total);
        System.out.println("Accuracy: " + accuracy);
        System.out.println("Precision: " + precision);
        System.out.println("Recall: " + recall);
        System.out.println("F1-score: " + f1);
    }
}
