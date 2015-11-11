package com.CSE512.GeospatialOperation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

//class Point implements Comparable<Point>, Serializable {
//
//	private static final long serialVersionUID = 1L;
//	public final double x_coordinate, y_coordinate;
//
//	public Point(double x, double y) {
//		this.x_coordinate = x;
//		this.y_coordinate = y;
//	}
//
//	public int compareTo(Point p) {
//		if (this.x_coordinate == p.x_coordinate) {
//			return (int) (this.y_coordinate - p.y_coordinate);
//		} else {
//			return (int) (this.x_coordinate - p.y_coordinate);
//		}
//	}
//
//	public String toString() {
//		return x_coordinate + "," + y_coordinate;
//	}
//
//	@Override
//	public boolean equals(Object o) {
//		if (o instanceof Point) {
//			Point compare = (Point) o;
//			return ((this.x_coordinate == compare.x_coordinate)) && ((this.y_coordinate == compare.y_coordinate));
//		}
//		return false;
//	}
//}

class Pair implements Serializable {
	private static final long serialVersionUID = 1L;
	public Points Point1 = null;
	public Points Point2 = null;
	public double distance = 0.0;

	public Pair(Points Point1, Points Point2) {
		this.Point1 = Point1;
		this.Point2 = Point2;
		calculateDistance();
	}

	public void calculateDistance() {
		double xdistance = Point2.x - Point1.x;
		double xsquare = Math.pow(xdistance, 2);
		double ydistance = Point2.y - Point1.y;
		double ysquare = Math.pow(ydistance, 2);
		this.distance = Math.sqrt(xsquare + ysquare);
	}

	public void edit(Points Point1, Points Point2, double distance) {
		this.Point1 = Point1;
		this.Point2 = Point2;
		this.distance = distance;
	}

	public String toString() {
		return Point1 + "-" + Point2 + ":" + distance;
	}
}

public class GeometryClosestPair {

	public static void sortByXcoordinate(List<Points> Points) {
		Collections.sort(Points, new Comparator<Points>() {
			public int compare(Points Point1, Points Point2) {
				if (Point1.x < Point2.x)
					return -1;
				if (Point1.x > Point2.x)
					return 1;
				return 0;
			}
		});
	}

	public static void sortByYcoordinate(List<Points> Points) {
		Collections.sort(Points, new Comparator<Points>() {
			public int compare(Points Point1, Points Point2) {
				if (Point1.y < Point2.y)
					return -1;
				if (Point1.y > Point2.y)
					return 1;
				return 0;
			}
		});
	}

	public static double pairdistance(Points p1, Points p2) {
		double xdistance = p2.x - p1.x;
		double xsquare = Math.pow(xdistance, 2);
		double ydistance = p2.y - p1.y;
		double ysquare = Math.pow(ydistance, 2);
		return Math.sqrt(xsquare + ysquare);
	}

	private static Pair divideAndConquerRecurse(List<Points> sorted_Xcoordinate, List<Points> sorted_Ycoordinate) {
		int NoOfPoints = sorted_Xcoordinate.size();
		if (NoOfPoints <= 3) {
			int NPoints = sorted_Xcoordinate.size();
			if (NPoints < 2) {
				return null;
			}
			Pair closestPair = new Pair(sorted_Xcoordinate.get(0), sorted_Xcoordinate.get(1));
			if (NPoints > 2) {
				for (int i = 0; i < NPoints - 1; i++) {
					Points Point1 = sorted_Xcoordinate.get(i);
					for (int j = i + 1; j < NPoints; j++) {
						Points Point2 = sorted_Xcoordinate.get(j);
						double distance = pairdistance(Point1, Point2);
						if (distance < closestPair.distance) {
							closestPair.edit(Point1, Point2, distance);
						}
					}
				}
			}
			return closestPair;
		}

		int dPoint = NoOfPoints >>> 1;
		List<Points> Part1 = sorted_Xcoordinate.subList(0, dPoint);
		List<Points> Part2 = sorted_Xcoordinate.subList(dPoint, NoOfPoints);

		List<Points> t_List = new ArrayList<Points>(Part1);
		sortByYcoordinate(t_List);
		Pair closest_Pair = divideAndConquerRecurse(Part1, t_List);

		t_List.clear();
		t_List.addAll(Part2);
		sortByYcoordinate(t_List);
		Pair closestPair_Part2 = divideAndConquerRecurse(Part2, t_List);

		if (closestPair_Part2.distance < closest_Pair.distance) {
			closest_Pair = closestPair_Part2;
		}

		t_List.clear();
		double min_Distance = closest_Pair.distance;
		double cX = Part2.get(0).x;
		for (Points Points : sorted_Ycoordinate)
			if (Math.abs(cX - Points.x) < min_Distance) {
				t_List.add(Points);
			}
		for (int i = 0; i < t_List.size() - 1; i++) {
			Points Point1 = t_List.get(i);
			for (int j = i + 1; j < t_List.size(); j++) {
				Points Point2 = t_List.get(j);
				if ((Point2.y - Point1.y) >= min_Distance)
					break;
				double distance = pairdistance(Point1, Point2);
				if (distance < closest_Pair.distance) {
					closest_Pair.edit(Point1, Point2, distance);
					min_Distance = distance;
				}
			}
		}
		return closest_Pair;
	}

	public static Pair divideAndConquer(List<Points> Points) {
		List<Points> sorted_Xcoordinate = new ArrayList<Points>(Points);
		sortByXcoordinate(sorted_Xcoordinate);
		List<Points> sorted_Ycoordinate = new ArrayList<Points>(Points);
		sortByYcoordinate(sorted_Ycoordinate);
		return divideAndConquerRecurse(sorted_Xcoordinate, sorted_Ycoordinate);
	}

	public static void main(String[] args) {
		String InputLocation = "hdfs://master:54310/data/ClosestPairTestData.csv";// args[0];
		String OutputLocation = "hdfs://master:54310/data/ClosestPair";// args[1];
		// String localIntermediateFile =
		// "hdfs://master:54310/data/ClosestPairIntermediateFile";
		JavaSparkContext sc = SContext.getJavaSparkContext();
		JavaRDD<String> lines = sc.textFile(InputLocation);
		JavaRDD<Points> length = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Points>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Points> call(Iterator<String> str) throws Exception {
				List<Points> Points = new ArrayList<Points>();
				List<Coordinate> Potential_Coordinates = new ArrayList<Coordinate>();
				List<Points> final_Pair = new ArrayList<Points>();
				GeometryFactory geomtryF = new GeometryFactory();
				while (str.hasNext()) {
					String str_Temp = str.next();
					String[] row = str_Temp.split(",");
					Points x = new Points(Double.parseDouble(row[0]), Double.parseDouble(row[1]));
					Coordinate coordinates = new Coordinate(Double.parseDouble(row[0]), Double.parseDouble(row[1]));
					Potential_Coordinates.add(coordinates);
					Points.add(x);
				}
				ConvexHull convex_hull = new ConvexHull(Potential_Coordinates
						.toArray(new Coordinate[Potential_Coordinates.size()]), geomtryF);
				Geometry geometry = convex_hull.getConvexHull();
				List<Coordinate> local_ConvexHull = Arrays.asList(geometry.getCoordinates());
				Pair local_ClosestPair = divideAndConquer(Points);

				if (!local_ConvexHull.contains(new Coordinate(local_ClosestPair.Point1.x, local_ClosestPair.Point1.y)))
					final_Pair.add(local_ClosestPair.Point1);
				if (!local_ConvexHull.contains(new Coordinate(local_ClosestPair.Point2.x, local_ClosestPair.Point2.y)))
					final_Pair.add(local_ClosestPair.Point2);
				int size = local_ConvexHull.size();
				for (int i = 0; i < size - 1; i++) {
					Coordinate c = local_ConvexHull.get(i);
					final_Pair.add(new Points(c.x, c.y));
				}
				return final_Pair;
			}
		});
		// length.saveAsTextFile(localIntermediateFile);
		JavaRDD<Points> ReduceList = length.repartition(1);
		JavaRDD<Points> FinalList = ReduceList.mapPartitions(new FlatMapFunction<Iterator<Points>, Points>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Points> call(Iterator<Points> Iter) {
				List<Points> Points = new ArrayList<Points>();
				while (Iter.hasNext()) {
					Points p = Iter.next();
					Points.add(p);
				}
				Pair global_ClosestPair = divideAndConquer(Points);
				List<Points> final_Points = new ArrayList<Points>();
				final_Points.add(global_ClosestPair.Point1);
				final_Points.add(global_ClosestPair.Point2);
				Collections.sort(final_Points);
				return final_Points;
			}
		});
		FinalList.saveAsTextFile(OutputLocation);
		sc.close();
	}
}