package com.example;

import java.util.Vector;

public class MyData {

    //向量维度
    private Integer dimension;
    //向量坐标
    private Vector<Double>vec = new Vector<Double>();
    //target属性
    private String  attr;

    public  void setAttr(String attr)
    {
        this.attr = attr;
    }

    public void setVec(Vector<Double> vec) {
        this.dimension = vec.size();
        for(Double d : vec)
        {
            this.vec.add(d);
        }
    }

    public double calDist(MyData data1)//计算两条数据之间的欧式距离
    {
        try{
            if(this.dimension != data1.dimension)
                throw new Exception("These two vectors have different dimensions.");

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        double dist = 0;
        for(int i = 0;i<dimension;i++)
        {
            dist += Math.pow(this.vec.get(i)-data1.vec.get(i),2);
        }
        dist = Math.sqrt(dist);
        return dist;
    }

    public String getAttr() {
        return attr;
    }
}

