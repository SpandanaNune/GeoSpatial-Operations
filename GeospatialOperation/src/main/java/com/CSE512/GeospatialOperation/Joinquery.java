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
    
	JavaRDD<String> inputFile1 = javasc.textFile(args[1]);
	JavaRDD<String> inputFile2 = javasc.textFile(args[2]);
	List<String> list=inputFile1.collect();
	String[] st= list.toArray(new String[0]);
	String[] value;

	Broadcast<String[]> br=javasc.broadcast(st);
	value=br.value();
    
   final String ar[]=value;
   
	JavaPairRDD<String,String> j=inputFile2.mapToPair(new PairFunction<String,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(String data)
		{
			double x1=0.0;
			double x2=0.0;
			double y1=0.0;
			double y2=0.0;
			String x = "" ;
			String bid="";
			String parts[]=data.split(",");
			bid=parts[1];
			if(parts.length==4){
			 x1=Double.parseDouble(parts[1]);
			 y1=Double.parseDouble(parts[2]);
			x2=Double.parseDouble(parts[3]);
			 y2=Double.parseDouble(parts[4]);
			}
			else{
				 x1=Double.parseDouble(parts[1]);
				 y1=Double.parseDouble(parts[2]);
			}
			
			
			for(String part: ar)
			{
				String str[]=part.split(",");
				double aid=Double.parseDouble(str[1]);
				double a1=Double.parseDouble(str[2]);
				double b1=Double.parseDouble(str[3]);
				double a2=Double.parseDouble(str[4]);
				double b2=Double.parseDouble(str[5]);
				
				if(((Math.max(a1, a2) > x1)||(Math.max(a1, a2) > x2))&&((Math.max(b1, b2) > y1)||(Math.max(b1, b2) > y2))&&((Math.min(a1, a2) < x1)||(Math.min(a1, a2) < x2))&&((Math.min(b1, b2) < y1)||(Math.min(b1, b2) < y2)))
				{
					x = x + "["+str[1]+"]";  
				}
			
			}
		
			return new Tuple2<String, String>(bid,x);
		}
	}).repartition(1);
	
    j.saveAsTextFile(args[3]);
    javasc.stop();
    javasc.close();
}

}

