package com.CSE512.GeospatialOperation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.GeometryFactory;


/**
 * Reduce points using Convex Hull and then apply brute force technique to find farthest point pairs.
 * @author Kunal Lakhwani
 *
 */

class ConvexHullCalculation implements FlatMapFunction<Iterator<String>, Coordinate>, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Iterable<Coordinate> call(Iterator<String> s) throws Exception {
		ArrayList<Coordinate> coords = new ArrayList<Coordinate>();
		while (s.hasNext()) {
			String[] fields = s.next().split(",");
			Coordinate coord = new Coordinate(Double.parseDouble(fields[0]), Double.parseDouble(fields[1]));
			coords.add(coord);
		}
		coords = ConvexHullOperation.prune(coords);
		ConvexHull ch = new ConvexHull(coords.toArray(new Coordinate[coords.size()]), new GeometryFactory());
		Iterable<Coordinate> chcords = Arrays.asList(ch.getConvexHull().getCoordinates());

		return chcords;
	};
}

public class FarthestPair {
	public static void main(String[] args) {
		JavaSparkContext sc = SContext.getJavaSparkContext();
		JavaRDD<String> lines = sc.textFile(args[1]);
		JavaRDD<Coordinate> listOfCoordinates = lines.mapPartitions(new ConvexHullCalculation());

		List<Coordinate> convexHullList = listOfCoordinates.collect();
		Coordinate point1,point2;
		point1 = convexHullList.get(0);
		point2 = convexHullList.get(1);
		
		double maxDistance = 0;

		for(int i = 0; i < (convexHullList.size()) - 1; i++) {
			for(int j = i+1; j < convexHullList.size(); j++) {
				double xCoOrdinate = convexHullList.get(i).x - convexHullList.get(j).x;
				double xDistance = Math.pow(xCoOrdinate, 2);
				double yCoOrdinate = convexHullList.get(i).y - convexHullList.get(j).y;
				double yDistance = Math.pow(yCoOrdinate, 2);
				double currentDistance = Math.sqrt(xDistance + yDistance);

				if(currentDistance > maxDistance) {
					maxDistance = currentDistance;
					point1 = convexHullList.get(i);
					point2 = convexHullList.get(j);
				}
			}
		}

		List<Coordinate> points = new ArrayList<Coordinate>();
		points.add(point1); points.add(point2);
		JavaRDD<Coordinate> farthestPair = sc.parallelize(points).repartition(1);
		farthestPair.saveAsTextFile(args[3]);
		sc.close();
	}
}