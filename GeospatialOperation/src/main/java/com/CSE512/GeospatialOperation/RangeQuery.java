package com.CSE512.GeospatialOperation;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class RangeQuery {

	public static void main(String[] args) {
		SparkConf configuration = new SparkConf().setAppName("Geospatial Application").setMaster(args[0]);    
		JavaSparkContext javasc = new JavaSparkContext(configuration);
		JavaRDD<String> inputFile1 = javasc.textFile(args[1]);
		JavaRDD<String> inputFile2 = javasc.textFile(args[2]);
		String val=inputFile1.first();
    	String[] windowpoints = val.split(",");
    	final Double[] querywindow = new Double[4];
    	for(int i=0; i<4; i++)
    	{
    		querywindow[i] = Double.parseDouble(windowpoints[i]);
    	}
    	
    	Broadcast<Double[]> broadcast = javasc.broadcast(querywindow);
    	final Double[] value = broadcast.value();
    	
    	
    	JavaPairRDD<String, String> enclosed = inputFile2.mapToPair(new PairFunction<String, String, String>()
    			{

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(String data)
    		        {
    		        	String parts[] = data.split(",");
    		        	double x1=Double.parseDouble(parts[0]);
    					double y1=Double.parseDouble(parts[1]);
    					double x2=Double.parseDouble(parts[2]);
    					double y2=Double.parseDouble(parts[3]);
    		        
    					if((Math.max(value[0],value[2])>x1)&&(Math.max(value[1],value[3])>y1)&&(Math.min(value[0],value[2])<x1)&&(Math.min(value[1],value[3])<y1))
    						return new Tuple2<String, String> (x1+","+y1+","+x2+","+y2, "");  
    						else
    						    return new Tuple2<String, String> ("NULL", "b");
    			
    		        }
    		        } );
    	
    	String outdata = "";
    	
    	List<Tuple2<String,String>> output = enclosed.collect();
    	List<String> list = new ArrayList<String>();
    	for(Tuple2<?,?> tuple: output)
    	{
    		if(!tuple._1().toString().contains("NULL"))
    		{
    			outdata += tuple._1() + "\n";
    		    list.add(tuple._1().toString());
    		}
    	}
    	
    	JavaRDD<String> out = javasc.parallelize(list).repartition(1);
    	out.saveAsTextFile(args[3]);
javasc.close();
        
    			
    			
	}

}
