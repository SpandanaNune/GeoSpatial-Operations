package com.CSE512.GeospatialOperation;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Used Graham Scan algorithm for implementing Convex Hull
 * 
 * @author Kulvir Gahlawat
 *
 */
public class ConvexHullAlgo {
	/**
	 * Computes the convex hull of all the geometries in this field. Call this
	 * method once.
	 */
	public void computeConvexHull(String InputLocation, String OutputLocation) {
		Coordinate[] actualCoords = new Coordinate[10];
		ConvexHull convexHull = new ConvexHull(actualCoords, new GeometryFactory());
		Coordinate[] chcords = convexHull.getConvexHull().getCoordinates();
	}
	
	public static void main(String args[]){
		System.out.println("maja");
		SparkConf configuration = new SparkConf().setAppName("Convex Hull").setMaster("spark://192.168.0.226:7077");    
		JavaSparkContext javasc = new JavaSparkContext(configuration);
		//Read a text file from HDFS and return it as an RDD of Strings.
		JavaRDD<String> inputFile = javasc.textFile("hdfs://master:54310/data/PolygonUnionTestData.csv");	
	}
}
