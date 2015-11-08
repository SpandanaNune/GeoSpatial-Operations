package com.CSE512.GeospatialOperation;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.*;

public class Joinquery {
public static void main(String[] args){
	SparkConf configuration = new SparkConf().setAppName("Geospatial Application").setMaster(args[0]);    
	JavaSparkContext javasc = new JavaSparkContext(configuration);
	String[] value;
	JavaRDD<String> inputFile1 = javasc.textFile(args[1]);
	JavaRDD<String> inputFile2 = javasc.textFile(args[2]);
	List<String> list=inputFile1.collect();
	String[] data= list.toArray(new String[0]);
	Broadcast<String[]> br=javasc.broadcast(data);
	value=br.value();  
    final String target[]=value;
   
	JavaPairRDD<String,String> out=inputFile2.mapToPair(new PairFunction<String,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(String window)
		{
			double p=0.0;
			double q=0.0;
			double r=0.0;
			double s=0.0;
			String output = "" ;
			String bid="";
			String windowpoints[]=window.split(",");
			bid=windowpoints[1];
			
			 p=Double.parseDouble(windowpoints[2]);
			 r=Double.parseDouble(windowpoints[3]);
			q=Double.parseDouble(windowpoints[4]);
			 s=Double.parseDouble(windowpoints[5]);
					
			for(String part: target)
			{
				String targetpoints[]=part.split(",");
				
				if(targetpoints.length==4){
				double x1=Double.parseDouble(targetpoints[2]);
				double y1=Double.parseDouble(targetpoints[3]);
				double x2=Double.parseDouble(targetpoints[4]);
				double y2=Double.parseDouble(targetpoints[5]);
				
				if((Math.max(p, q) > Math.max(x1, x2))&&(Math.max(r, s) > Math.max(y1, y2))&&(Math.min(p, q) < Math.min(x1, x2))&&(Math.min(r, s) < Math.min(y1, y2)))
				{
					output = output + "["+targetpoints[1]+"]";  
				}
				}
				else{
					double x1=Double.parseDouble(targetpoints[2]);
					double y1=Double.parseDouble(targetpoints[3]);
				if((Math.max(p,q)>x1)&&(Math.max(r,s)>y1)&&(Math.min(p, q)<x1)&&(Math.min(r, s)<y1)){
					output = output + "["+targetpoints[1]+"]";  
				}
				}
			
			}
		
			return new Tuple2<String, String>(bid,output);
		}
	}).repartition(1);
	
    out.saveAsTextFile(args[3]);
    javasc.stop();
    javasc.close();
}
}

