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

public class SpatialRange {

	public static void main(String[] args) {
		
		/*SparkConf config = new SparkConf().setMaster(args[0]);
		JavaSparkContext javaSparkContext = new JavaSparkContext(config);*/
		JavaSparkContext javaSparkContext = SContext.getJavaSparkContext();

		JavaRDD<String> rects = javaSparkContext.textFile(args[1]);
    	String result=rects.first();
    	String[] windowArray = result.split(",");
    	Double[] window = new Double[4];
    	for(int i=0; i<4; i++)
    	{
    		window[i] = Double.parseDouble(windowArray[i]);
    	}
    	
    	Broadcast<Double[]> br = javaSparkContext.broadcast(window);
    	final Double[] broad = br.value();
    	
    	rects = javaSparkContext.textFile(args[2]);
    	JavaPairRDD<String, String> enclosed = rects.mapToPair(new PairFunction<String, String, String>()
    			{

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(String data)
    		        {
    		        	String parts[] = data.split(",");
    		        	double x1=Double.parseDouble(parts[0]);
    					double y1=Double.parseDouble(parts[1]);
    					double x2=Double.parseDouble(parts[2]);
    					double y2=Double.parseDouble(parts[3]);
    					
    					double fx1=x1;
    					double fy1=y1;
    					double fx2=x2;
    					double fy2=y2;
    					
    					if(x1 > x2)
    					{
    						fx1 = x2;
    						fx2 = x1;
    					}
    					if(y1 > y2)
    					{
    						fy1 = y2;
    						fy2 = y1;
    					}
    					
    					if((fx1 >= broad[0] && fx2 <= broad[2]) && (fy1 >= broad[1] && fy2 <= broad[3]))
						{
							return new Tuple2<String, String> (x1+","+y1+","+x2+","+y2, "");
						}       
    					else
						    return new Tuple2<String, String> ("NULL", "b");
    		        }
    			});
    	
    	String data = "";
    	
    	List<Tuple2<String,String>> output = enclosed.collect();
    	List<String> srdd = new ArrayList<String>();
    	for(Tuple2<?,?> tuple: output)
    	{
    		if(!tuple._1().toString().contains("NULL"))
    		{
    		    data += tuple._1() + "\n";
    		    srdd.add(tuple._1().toString());
    		}
    	}
    	
    	JavaRDD<String> op = javaSparkContext.parallelize(srdd).repartition(1);
    	op.saveAsTextFile(args[3]);
    	javaSparkContext.close();
        
    }


}
