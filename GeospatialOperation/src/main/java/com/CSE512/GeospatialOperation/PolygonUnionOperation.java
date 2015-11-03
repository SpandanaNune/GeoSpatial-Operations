package com.CSE512.GeospatialOperation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import com.vividsolutions.jts.geom.Geometry;

public class PolygonUnionOperation {
	
	
	static void main(String args[]){
		
		SparkConf configuration = new SparkConf().setAppName("Geospatial Application").setMaster(args[0]);    
		JavaSparkContext sc = new JavaSparkContext(configuration);
	    
		//Read a text file from HDFS and return it as an RDD of Strings.
		JavaRDD<String> inputFile1 = sc.textFile(args[1]);
	//	JavaRDD<Geometry> localUnionPolygon = inputFile1.mapPartitions(new LocalUnion());
		
	//	localUnionPolygon.saveAsTextFile(args[2]);
		
		//Return a new RDD that has exactly 1 partition.
		/*JavaRDD<Geometry> partionList = localUnionPolygon.repartition(1);
		JavaRDD<Geometry> globalUnionPolygon = partionList.mapPartitions(new GlobalUnion());
		
		globalUnionPolygon.saveAsTextFile(args[3]);*/
		
	}

}
