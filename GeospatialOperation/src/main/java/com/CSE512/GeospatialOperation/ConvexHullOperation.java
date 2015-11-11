package com.CSE512.GeospatialOperation;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

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
	@SuppressWarnings("unchecked")
	public static ArrayList<Coordinate> prune(ArrayList<Coordinate> coords) {
		Coordinate a = new Coordinate();
		Coordinate b = new Coordinate();
		Coordinate c = new Coordinate();
		Coordinate d = new Coordinate();
		boolean flag = true;
		Double x1, x2, y1, y2;
		for (Coordinate t : coords) {
			if (flag) {
				c.x = t.x;
				c.y = t.y;
				d.x = t.x;
				d.y = t.y;
				flag = false;
			}
			if (t.x - t.y > a.x - a.y) {
				a.x = t.x;
				a.y = t.y;
			}
			if (t.x + t.y > b.x + b.y) {
				b.x = t.x;
				b.y = t.y;
			}
			if (t.x + t.y < c.x + c.y) {
				c.x = t.x;
				c.y = t.y;
			}
			if (t.x + t.y < d.x + d.y) {
				d.x = t.x;
				d.y = t.y;
			}
		}
		x1 = Math.max(c.x, d.x);
		x2 = Math.min(a.x, b.x);
		y1 = Math.max(a.y, d.y);
		y2 = Math.min(b.y, c.y);
		for (int i = 0; i < coords.size(); i++) {
			Coordinate t = coords.get(i);
			if ((t.x > x1 && t.x < x2) && (t.y > y1 && t.y < y2)) {
				coords.remove(i--);
			}
		}
		Collections.sort(coords);
		return coords;
	}

	public static void main(String args[]) {
		String InputLocation = "hdfs://master:54310/data/ConvexHullTestData.csv";
		String OutputLocation = "hdfs://master:54310/data/ConvexHull";
		JavaSparkContext sc = SContext.getJavaSparkContext();
		JavaRDD<String> file = sc.textFile(InputLocation);
		JavaRDD<Coordinate> chcords = file.mapPartitions(new FlatMapFunction<Iterator<String>, Coordinate>() {
			private static final long serialVersionUID = 11111L;

			public Iterable<Coordinate> call(Iterator<String> s) throws Exception {
				ArrayList<Coordinate> coords = new ArrayList<Coordinate>();
				while (s.hasNext()) {
					String[] fields = s.next().split(",");
					Coordinate coord = new Coordinate(Double.parseDouble(fields[0]), Double.parseDouble(fields[1]));
					coords.add(coord);
				}
				coords = prune(coords);
				ConvexHull chLocal = new ConvexHull(coords.toArray(new Coordinate[coords.size()]),
						new GeometryFactory());
				List<Coordinate> chcords = Arrays.asList(chLocal.getConvexHull().getCoordinates());
				List<Coordinate> temp = new ArrayList<Coordinate>(chcords);
				Coordinate c = new Coordinate(chcords.get(0).x, chcords.get(0).y);
				temp.add(c);
				chcords = Arrays.asList(new GeometryFactory().createPolygon(temp.toArray(new Coordinate[temp.size()])).convexHull().getCoordinates());
				HashSet<Coordinate> h = new HashSet<Coordinate>();
				h.addAll(chcords);
				chcords = new ArrayList<Coordinate>();
				chcords.addAll(h);
				Collections.sort(chcords);
				return chcords;
			}
		});
		chcords.saveAsTextFile(OutputLocation);
	}
}
