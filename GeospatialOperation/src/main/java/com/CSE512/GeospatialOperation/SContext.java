package com.CSE512.GeospatialOperation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SContext {

	public static volatile JavaSparkContext sc;
	public static final String JAR = "/home/kulvir/Downloads/sparktest.jar"; 
	public static final String APP = "GeoSpatialOperations";
	public static final String MASTER = "spark://192.168.0.226:7077";

	public static synchronized JavaSparkContext getJavaSparkContext() {
		
		if (sc == null) {
			SparkConf conf = new SparkConf().setAppName(APP).setMaster(MASTER);
			conf.setJars(new String[]{"/home/kulvir/Downloads/sparktest.jar", "/home/kulvir/Downloads/jts-1.13.jar"});
			sc = new JavaSparkContext(conf);
			sc.addJar(JAR);			
		}
		return sc;
	}
}
