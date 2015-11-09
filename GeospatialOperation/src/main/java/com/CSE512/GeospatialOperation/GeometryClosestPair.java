package com.CSE512.GeospatialOperation;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;


class Point implements Comparable<Point>,Serializable {
	
	private static final long serialVersionUID = 1L;
	public final double x_coordinate, y_coordinate;
	public Point(double x, double y){
		this.x_coordinate = x;
		this.y_coordinate = y;
	}
	public int compareTo(Point p) {
		if(this.x_coordinate == p.x_coordinate){
			return (int) (this.y_coordinate - p.y_coordinate);
		} else {
			return (int) (this.x_coordinate - p.y_coordinate);
		}
	}	
	public String toString(){
		return x_coordinate+","+y_coordinate;
	}
	@Override
	public boolean equals(Object o){
		if(o instanceof Point){
			Point compareTo = (Point) o;
			return ((this.x_coordinate==compareTo.x_coordinate))&&((this.y_coordinate==compareTo.y_coordinate));
		}
		return false;
	}
}

class Pair implements Serializable{
	private static final long serialVersionUID = 1L;
	public Point point1 = null;
	public Point point2 = null;
	public double distance = 0.0;
	
	public Pair(Point point1, Point point2){
		this.point1 = point1;
		this.point2 = point2;
		calculateDistance();
	}
	
	public void update(Point point1, Point point2, double distance){
	    this.point1 = point1;
	    this.point2 = point2;
	    this.distance = distance;
	  }

	  public void calculateDistance(){  	
		  double xdistance = point2.x_coordinate - point1.x_coordinate;
		  double xsquare = Math.pow(xdistance,2);
		  double ydistance = point2.y_coordinate - point1.y_coordinate;
		  double ysquare = Math.pow(ydistance,2);
		  this.distance =   Math.sqrt(xsquare+ysquare); 
	  }
	  
	  public String toString(){
		  return point1 + "-" + point2 + ":" + distance;
	  }
}

public class GeometryClosestPair {
	
	public static void sortByXcoordinate(List<Point> points){
		Collections.sort(points, new Comparator<Point>(){
			public int compare(Point point1, Point point2) {
				if(point1.x_coordinate < point2.y_coordinate) return -1;
				if(point1.x_coordinate > point2.y_coordinate) return 1;
				return 0;
			}
		});
	}

	public static void sortByYcoordinate(List<Point> points){
		Collections.sort(points, new Comparator<Point>(){
			public int compare(Point point1, Point point2) {
				if(point1.y_coordinate < point2.y_coordinate) return -1;
				if(point1.y_coordinate > point2.y_coordinate) return 1;
				return 0;
			}
		});
	}
	
	public static double pairdistance(Point p1, Point p2){
	  double xdistance = p2.x_coordinate - p1.x_coordinate;
	  double xsquare = Math.pow(xdistance,2);
	  double ydistance = p2.y_coordinate - p1.y_coordinate;
	  double ysquare = Math.pow(ydistance,2);
	  return Math.sqrt(xsquare+ysquare); 
	}
	
	public static Pair divideAndConquer(List<Point> Points){
	    List<Point> sorted_Xcoordinate = new ArrayList<Point>(Points);
	    sortByXcoordinate(sorted_Xcoordinate);
	    List<Point> sorted_Ycoordinate = new ArrayList<Point>(Points);
	    sortByYcoordinate(sorted_Ycoordinate);
	    return divideAndConquerRecurse(sorted_Xcoordinate, sorted_Ycoordinate);
	  }
	
	public static Pair bruteForceApproach(List<Point> Points){
	    int NoOfPoints = Points.size();
	    if (NoOfPoints < 2){
	      return null;
	    }
	    Pair closestPair = new Pair(Points.get(0), Points.get(1));
	    if (NoOfPoints > 2)
	    {
	      for (int i = 0; i < NoOfPoints - 1; i++)
	      {
	        Point point1 = Points.get(i);
	        for (int j = i + 1; j < NoOfPoints; j++)
	        {
	          Point point2 = Points.get(j);
	          double distance = pairdistance(point1, point2);
	          if (distance < closestPair.distance){
	        	  closestPair.update(point1, point2, distance);
	          }
	        }
	      }
	    }
	    return closestPair;
	  }
	
	  private static Pair divideAndConquerRecurse(List<Point> sorted_Xcoordinate, List<Point> sorted_Ycoordinate){
	    int NoOfPoints = sorted_Xcoordinate.size();
	    if (NoOfPoints <= 3){
	      return bruteForceApproach(sorted_Xcoordinate);
	    }
	    
	    int dPoint = NoOfPoints >>> 1;
	    List<Point> Part1 = sorted_Xcoordinate.subList(0, dPoint);
	    List<Point> Part2 = sorted_Xcoordinate.subList(dPoint, NoOfPoints);
	 
	    List<Point> t_List = new ArrayList<Point>(Part1);
	    sortByYcoordinate(t_List);
	    Pair closestPair = divideAndConquerRecurse(Part1, t_List);
	 
	    t_List.clear();
	    t_List.addAll(Part2);
	    sortByYcoordinate(t_List);
	    Pair closestPairRight = divideAndConquerRecurse(Part2, t_List);
	 
	    if (closestPairRight.distance < closestPair.distance){
	      closestPair = closestPairRight;
	    }
	 
	    t_List.clear();
	    double shortestDistance =closestPair.distance;
	    double centerX = Part2.get(0).x_coordinate;
	    for (Point point : sorted_Ycoordinate)
	      if (Math.abs(centerX - point.x_coordinate) < shortestDistance){
	    	  t_List.add(point);
	      }
	    for (int i = 0; i < t_List.size() - 1; i++){
	      Point point1 = t_List.get(i);
	      for (int j = i + 1; j < t_List.size(); j++){
	        Point point2 = t_List.get(j);
	        if ((point2.y_coordinate - point1.y_coordinate) >= shortestDistance)
	          break;
	        double distance = pairdistance(point1, point2);
	        if (distance < closestPair.distance){
	          closestPair.update(point1, point2, distance);
	          shortestDistance = distance;
	        }
	      }
	    }
	    return closestPair;
	  }
	  

	public static void main(String[] args) {
		System.out.println("Main method");
		SparkConf conf = new SparkConf().setAppName("App").setMaster(args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[1]);
		System.out.println("RDD created");
		JavaRDD<Point> linelengths = lines.mapPartitions(new FlatMapFunction<Iterator<String>,Point>(){
			private static final long serialVersionUID = 1L;

			public Iterable<Point> call(Iterator<String> s) throws Exception {
				List<Point> points = new ArrayList<Point>();
				List<Coordinate> ActiveCoords = new ArrayList<Coordinate>();
				List<Point> finalPairAndConvexHull = new ArrayList<Point>();
				GeometryFactory geom = new GeometryFactory();
				while(s.hasNext())
				{
					String strTemp = s.next();
					String[] fields = strTemp.split(",");
					Point x=new Point(Double.parseDouble(fields[0]),Double.parseDouble(fields[1]));
					Coordinate coord = new Coordinate(Double.parseDouble(fields[0]),Double.parseDouble(fields[1]));
					ActiveCoords.add(coord);
					points.add(x);
				}
				ConvexHull ch = new ConvexHull(ActiveCoords.toArray(new Coordinate[ActiveCoords.size()]), geom);
				Geometry g=ch.getConvexHull();
				List<Coordinate> localConvexHull =  Arrays.asList(g.getCoordinates());
				Pair localClosestPair=divideAndConquer(points);
				System.out.println(" point 1 "+localClosestPair.point1.x_coordinate+ " "+localClosestPair.point1.y_coordinate);
				System.out.println(" point 2 "+localClosestPair.point2.x_coordinate+ " "+localClosestPair.point2.y_coordinate);
				
				System.out.println(localConvexHull.contains(new Coordinate(localClosestPair.point1.x_coordinate,localClosestPair.point1.y_coordinate)));
				System.out.println(localConvexHull.contains(new Coordinate(localClosestPair.point2.x_coordinate,localClosestPair.point2.y_coordinate)));
				
				if(!localConvexHull.contains(new Coordinate(localClosestPair.point1.x_coordinate,localClosestPair.point1.y_coordinate)))
					finalPairAndConvexHull.add(localClosestPair.point1);
				if(!localConvexHull.contains(new Coordinate(localClosestPair.point2.x_coordinate,localClosestPair.point2.y_coordinate)))	
					finalPairAndConvexHull.add(localClosestPair.point2);
				int siz=localConvexHull.size();
				for(int i=0;i<siz-1;i++)
				{
					Coordinate c =localConvexHull.get(i);
					finalPairAndConvexHull.add(new Point(c.x,c.y));
				}
				return finalPairAndConvexHull;
			}
		});
		linelengths.saveAsTextFile(args[2]);
		JavaRDD<Point> ReduceList = linelengths.repartition(1);
		JavaRDD<Point> FinalList = ReduceList.mapPartitions(new FlatMapFunction<Iterator<Point>, Point>()
		{
			private static final long serialVersionUID = 1L;

			public Iterable<Point> call(Iterator<Point> givListIter)
			{
				List<Point> points=new ArrayList<Point>();
				while(givListIter.hasNext())
				{
					Point p = givListIter.next();
					points.add(p);
				}
				Pair globalClosestPair=divideAndConquer(points);
				List<Point> finalPoints=new ArrayList<Point>();
				finalPoints.add(globalClosestPair.point1);
				finalPoints.add(globalClosestPair.point2);
				return finalPoints;
			}
		});
		FinalList.saveAsTextFile(args[3]);
		sc.close();
    }
}