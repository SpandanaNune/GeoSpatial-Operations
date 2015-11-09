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

class Point implements Comparable<Point>, Serializable {

	private static final long serialVersionUID = 1L;
	public final double x_coordinate, y_coordinate;

	public Point(double x, double y) {
		this.x_coordinate = x;
		this.y_coordinate = y;
	}

	public int compareTo(Point p) {
		if (this.x_coordinate == p.x_coordinate) {
			return (int) (this.y_coordinate - p.y_coordinate);
		} else {
			return (int) (this.x_coordinate - p.y_coordinate);
		}
	}

	public String toString() {
		return x_coordinate + "," + y_coordinate;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Point) {
			Point compareTo = (Point) o;
			return ((this.x_coordinate == compareTo.x_coordinate)) && ((this.y_coordinate == compareTo.y_coordinate));
		}
		return false;
	}
}

class Pair implements Serializable {
	private static final long serialVersionUID = 1L;
	public Point point1 = null;
	public Point point2 = null;
	public double distance = 0.0;

	public Pair(Point point1, Point point2) {
		this.point1 = point1;
		this.point2 = point2;
		calculateDistance();
	}

	public void update(Point point1, Point point2, double distance) {
		this.point1 = point1;
		this.point2 = point2;
		this.distance = distance;
	}

	public void calculateDistance() {
		double xdistance = point2.x_coordinate - point1.x_coordinate;
		double xsquare = Math.pow(xdistance, 2);
		double ydistance = point2.y_coordinate - point1.y_coordinate;
		double ysquare = Math.pow(ydistance, 2);
		this.distance = Math.sqrt(xsquare + ysquare);
	}

	public String toString() {
		return point1 + "-" + point2 + ":" + distance;
	}
}

public class GeometryClosestPair {

	public static void sortByXcoordinate(List<Point> points) {
		Collections.sort(points, new Comparator<Point>() {
			public int compare(Point point1, Point point2) {
				if (point1.x_coordinate < point2.y_coordinate)
					return -1;
				if (point1.x_coordinate > point2.y_coordinate)
					return 1;
				return 0;
			}
		});
	}

	public static void sortByYcoordinate(List<Point> points) {
		Collections.sort(points, new Comparator<Point>() {
			public int compare(Point point1, Point point2) {
				if (point1.y_coordinate < point2.y_coordinate)
					return -1;
				if (point1.y_coordinate > point2.y_coordinate)
					return 1;
				return 0;
			}
		});
	}

	public static double pairdistance(Point p1, Point p2) {
		double xdistance = p2.x_coordinate - p1.x_coordinate;
		double xsquare = Math.pow(xdistance, 2);
		double ydistance = p2.y_coordinate - p1.y_coordinate;
		double ysquare = Math.pow(ydistance, 2);
		return Math.sqrt(xsquare + ysquare);
	}

	public static Pair divideAndConquer(List<Point> Points) {
		List<Point> sorted_Xcoordinate = new ArrayList<Point>(Points);
		sortByXcoordinate(sorted_Xcoordinate);
		List<Point> sorted_Ycoordinate = new ArrayList<Point>(Points);
		sortByYcoordinate(sorted_Ycoordinate);
		return divideAndConquerRecurse(sorted_Xcoordinate, sorted_Ycoordinate);
	}

	// public static Pair bruteForceApproach(List<Point> Points){
	// int NoOfPoints = Points.size();
	// if (NoOfPoints < 2){
	// return null;
	// }
	// Pair closestPair = new Pair(Points.get(0), Points.get(1));
	// if (NoOfPoints > 2)
	// {
	// for (int i = 0; i < NoOfPoints - 1; i++)
	// {
	// Point point1 = Points.get(i);
	// for (int j = i + 1; j < NoOfPoints; j++)
	// {
	// Point point2 = Points.get(j);
	// double distance = pairdistance(point1, point2);
	// if (distance < closestPair.distance){
	// closestPair.update(point1, point2, distance);
	// }
	// }
	// }
	// }
	// return closestPair;
	// }

	private static Pair divideAndConquerRecurse(List<Point> sorted_Xcoordinate, List<Point> sorted_Ycoordinate) {
		int NoOfPoints = sorted_Xcoordinate.size();
		if (NoOfPoints <= 3) {
			int No_Points = sorted_Xcoordinate.size();
			if (No_Points < 2) {
				return null;
			}
			Pair closestPair = new Pair(sorted_Xcoordinate.get(0), sorted_Xcoordinate.get(1));
			if (No_Points > 2) {
				for (int i = 0; i < NoOfPoints - 1; i++) {
					Point point1 = sorted_Xcoordinate.get(i);
					for (int j = i + 1; j < NoOfPoints; j++) {
						Point point2 = sorted_Xcoordinate.get(j);
						double distance = pairdistance(point1, point2);
						if (distance < closestPair.distance) {
							closestPair.update(point1, point2, distance);
						}
					}
				}
			}
			return closestPair;
		}

		int dPoint = NoOfPoints >>> 1;
		List<Point> Part1 = sorted_Xcoordinate.subList(0, dPoint);
		List<Point> Part2 = sorted_Xcoordinate.subList(dPoint, NoOfPoints);

		List<Point> t_List = new ArrayList<Point>(Part1);
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
		double cX = Part2.get(0).x_coordinate;
		for (Point point : sorted_Ycoordinate)
			if (Math.abs(cX - point.x_coordinate) < min_Distance) {
				t_List.add(point);
			}
		for (int i = 0; i < t_List.size() - 1; i++) {
			Point point1 = t_List.get(i);
			for (int j = i + 1; j < t_List.size(); j++) {
				Point point2 = t_List.get(j);
				if ((point2.y_coordinate - point1.y_coordinate) >= min_Distance)
					break;
				double distance = pairdistance(point1, point2);
				if (distance < closest_Pair.distance) {
					closest_Pair.update(point1, point2, distance);
					min_Distance = distance;
				}
			}
		}
		return closest_Pair;
	}

	public static void main(String[] args) {
		String InputLocation = "hdfs://master:54310/data/ClosestPairTestData.csv";
		String OutputLocation = "hdfs://master:54310/data/ClosestPair";
		String localIntermediateFile = "hdfs://master:54310/data/ClosestPairIntermediateFile";
		JavaSparkContext sc = SContext.getJavaSparkContext();
		JavaRDD<String> lines = sc.textFile(InputLocation);
		JavaRDD<Point> length = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Point>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Point> call(Iterator<String> str) throws Exception {
				List<Point> points = new ArrayList<Point>();
				List<Coordinate> Potential_Coordinates = new ArrayList<Coordinate>();
				List<Point> final_Pair = new ArrayList<Point>();
				GeometryFactory geomtryF = new GeometryFactory();
				while (str.hasNext()) {
					String strTemp = str.next();
					String[] row = strTemp.split(",");
					Point x = new Point(Double.parseDouble(row[0]), Double.parseDouble(row[1]));
					Coordinate coordinates = new Coordinate(Double.parseDouble(row[0]), Double.parseDouble(row[1]));
					Potential_Coordinates.add(coordinates);
					points.add(x);
				}
				ConvexHull convex_hull = new ConvexHull(Potential_Coordinates
						.toArray(new Coordinate[Potential_Coordinates.size()]), geomtryF);
				Geometry geometry = convex_hull.getConvexHull();
				List<Coordinate> local_ConvexHull = Arrays.asList(geometry.getCoordinates());
				Pair local_ClosestPair = divideAndConquer(points);

				if (!local_ConvexHull.contains(new Coordinate(local_ClosestPair.point1.x_coordinate,
						local_ClosestPair.point1.y_coordinate)))
					final_Pair.add(local_ClosestPair.point1);
				if (!local_ConvexHull.contains(new Coordinate(local_ClosestPair.point2.x_coordinate,
						local_ClosestPair.point2.y_coordinate)))
					final_Pair.add(local_ClosestPair.point2);
				int size = local_ConvexHull.size();
				for (int i = 0; i < size - 1; i++) {
					Coordinate c = local_ConvexHull.get(i);
					final_Pair.add(new Point(c.x, c.y));
				}
				return final_Pair;
			}
		});
		length.saveAsTextFile(localIntermediateFile);
		JavaRDD<Point> ReduceList = length.repartition(1);
		JavaRDD<Point> FinalList = ReduceList.mapPartitions(new FlatMapFunction<Iterator<Point>, Point>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Point> call(Iterator<Point> givListIter) {
				List<Point> points = new ArrayList<Point>();
				while (givListIter.hasNext()) {
					Point p = givListIter.next();
					points.add(p);
				}
				Pair globalClosestPair = divideAndConquer(points);
				List<Point> finalPoints = new ArrayList<Point>();
				finalPoints.add(globalClosestPair.point1);
				finalPoints.add(globalClosestPair.point2);
				Collections.sort(finalPoints);
				return finalPoints;
			}
		});
		FinalList.saveAsTextFile(OutputLocation);
		sc.close();
	}
}