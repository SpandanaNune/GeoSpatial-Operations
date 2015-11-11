/*package edu.asu.cse512;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SContext {

	public static volatile JavaSparkContext sc;
	static final String JAR = "/home/kulvir/Downloads/sparktest.jar"; 
	static final String APP = "GeoSpatialOperations";
	static final String MASTER = "spark://192.168.0.226:7077";

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
*/