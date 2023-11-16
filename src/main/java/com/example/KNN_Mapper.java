package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import javafx.util.Pair;

public class KNN_Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private Text text = new Text();//输出Val值

    private LongWritable longWritable = new LongWritable();
    private LongWritable TP= new LongWritable();
    private LongWritable FP= new LongWritable();
    private LongWritable TN= new LongWritable();
    private LongWritable FN= new LongWritable();
    private Integer K;//K值

    private Configuration conf;//全局配置
    private Integer dimension;//维度
    private List<MyData> training_data = new ArrayList<>();
    
    
    private void readTrainingData(URI uri)//读取训练数据到training_data中
    {
        System.err.println("Read Training Data");
        try{
            Path patternsPath = new Path(uri.getPath());
            String patternsFileName = patternsPath.getName().toString();
            BufferedReader reader = new BufferedReader(new FileReader(
                    patternsFileName));
            String line;
            Vector<Double>vec = new Vector<>();
            while ((line = reader.readLine()) != null) {

                String[] strings = line.split(",");

                for(int i=0;i<dimension;i++)
                {
                    vec.add(Double.valueOf(strings[i]));
                }
                MyData myData = new MyData();
                myData.setVec(vec);
                myData.setAttr(strings[dimension]);
                training_data.add(myData);
                vec.clear();
            }
            reader.close();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        System.err.println("Read End");
    }

    private double Gaussian(double dist)
    {
        //a = 1,b=0,c = 0.9,2*c^2 = 1.62
        double weight = Math.exp(-Math.pow(dist,2)/(1.62));
        return weight;
    }
    
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {

        conf = context.getConfiguration();
        this.K = conf.getInt("K",1);
        this.dimension = conf.getInt("dimension",1);
        URI[] uri = context.getCacheFiles();
        readTrainingData(uri[0]);
    }

    @Override
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();
        
        try {
            String[] strings = line.split(",");            
            
            if (strings .length!=dimension+1) {
                throw new Exception("Error line format in the table.");
            }

            //获取测试数据信息
            Vector<Double>vec = new Vector<>();
            for(String s:strings)
            {
                vec.add(Double.valueOf(s));
            }
            vec.remove(vec.size() - 1);
            MyData testData = new MyData();
            testData.setVec(vec);

            //计算与样本的K近邻

            //存放K近邻的优先级队列，元素类型为<距离，属性>
            PriorityQueue<Pair<Double,String>>K_nearst = new PriorityQueue<>((a,b)->(a.getKey()>b.getKey())?-1:1);
            double dist;
            for(MyData data : this.training_data)
            {
                dist = testData.calDist(data);
                if(K_nearst.size()<this.K)
                {
                    K_nearst.add(new Pair<>(dist,data.getAttr()));
                }
                else{
                    if(dist < K_nearst.peek().getKey())
                    {
                        K_nearst.poll();
                        K_nearst.add(new Pair<>(dist,data.getAttr()));
                    }
                }
            }

            //获取到K近邻后，通过高斯函数处理每条数据，并累加相同属性的权值，通过Hash_table实现
            Hashtable<String,Double>weightTable = new Hashtable<>();
            while(!K_nearst.isEmpty())
            {
                double d = K_nearst.peek().getKey();
                String attr = K_nearst.peek().getValue();
                double w = this.Gaussian(d);
                if(!weightTable.contains(attr))
                {
                    weightTable.put(attr,w);

                }
                else{
                    weightTable.put(attr,weightTable.get(attr)+w);
                }
                K_nearst.poll();
            }

            //选取权重最大的标签作为输出
            Double max_weight = Double.MIN_VALUE;
            String predict_attr = "";

            for(Iterator<String> itr = weightTable.keySet().iterator();itr.hasNext();){
                String hash_key = (String)itr.next();
                Double hash_val = weightTable.get(hash_key);
                if(hash_val > max_weight)
                {
                    predict_attr= hash_key;
                    max_weight = hash_val;
                }
            }
            String true_attr =strings[dimension];
            text.set(predict_attr);
           //
           TP.set(conf.getLong("TP",0));
           FP.set(conf.getLong("FP",0));
           TN.set(conf.getLong("TN",0));
           FN.set(conf.getLong("FN",0));
           if (predict_attr.equals("1") && true_attr.equals("1"))
                conf.setLong("TP", TP.get() + 1);
            if (predict_attr.equals("1") && true_attr.equals("0"))
                conf.setLong("FP", FP.get() + 1);
            if (predict_attr.equals("0") && true_attr.equals("0"))
                conf.setLong("TN", TN.get() + 1);
            if (predict_attr.equals("0") && true_attr.equals("1"))
                conf.setLong("FN", FN.get() + 1);
            //获取测试数据条数，用作下标计数
            longWritable.set(conf.getLong("testDataNum",0));
            conf.setLong("testDataNum",longWritable.get()+1);//计数加一
            context.write(longWritable,text);
            float total = (float) longWritable.get();
            if(total==61245)
            {
                float tp = (float) TP.get();
                float tn = (float) TN.get();
                float fp = (float) FP.get();
                float fn = (float) FN.get();
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
        catch (Exception e) {
            System.err.println(e.toString());
            System.exit(-1);
        }
    }
}

