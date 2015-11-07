package com.CSE512.GeospatialOperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Used Graham Scan algorithm for implementing Convex Hull
 * 
 * @author Kulvir Gahlawat
 *
 */
public class ConvexHullOperation {
	/**
	 * Computes the convex hull of all the geometries in this field. Call this
	 * method once.
	 */
	public void computeConvexHull() {
	}

	public static void main(String args[]) {
		String InputLocation = "hdfs://master:54310/data/ConvexHullTestData.csv";
		String OutputLocation = "hdfs://master:54310/data/ConvexHull";
		System.out.println("maja");
		JavaSparkContext sc = SContext.getJavaSparkContext();

		JavaRDD<String> file = sc.textFile(InputLocation);
		JavaRDD<Coordinate> chcords = file.mapPartitions(new FlatMapFunction<Iterator<String>, Coordinate>() {
			private static final long serialVersionUID = 11111L;

			public Iterable<Coordinate> call(Iterator<String> s) throws Exception {
				List<Coordinate> ActiveCoords = new ArrayList<Coordinate>();
				while (s.hasNext()) {
					String[] fields = s.next().split(",");
					Coordinate coord = new Coordinate(Double.parseDouble(fields[0]), Double.parseDouble(fields[1]));
					ActiveCoords.add(coord);
				}
				// Find Convex Hull
				ConvexHull ch = new ConvexHull(ActiveCoords.toArray(new Coordinate[ActiveCoords.size()]),
						new GeometryFactory());
				List<Coordinate> chcords = Arrays.asList(ch.getConvexHull().getCoordinates());
				return chcords;
			}
		});
		chcords.saveAsTextFile(OutputLocation);
		System.out.println("maja pura hua");
	}
}
