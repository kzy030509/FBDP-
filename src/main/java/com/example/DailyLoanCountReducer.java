package com.example;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Collections;

public class DailyLoanCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, String> resultMap;

    public void setup(Context context) {
        resultMap = new TreeMap<>(Collections.reverseOrder());
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        resultMap.put(sum, key.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, String> entry : resultMap.entrySet()) {
            context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
        }
    }
}