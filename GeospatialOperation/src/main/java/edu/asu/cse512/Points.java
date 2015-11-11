package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.vividsolutions.jts.geom.Coordinate;

public class Points implements Serializable, Comparable<Points> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public double x, y;

	public Points(double x, double y) {
		this.x = x;
		this.y = y;
	}

	public static ArrayList<Points> getPoints(List<Coordinate> coords) {
		ArrayList<Points> p = new ArrayList<Points>();
		for (Coordinate c : coords) {
			p.add(new Points(c.x, c.y));
		}
		return p;
	}

	@SuppressWarnings("unchecked")
	public static JavaRDD<Points> sortAndRemoveDuplicates(Coordinate[] c, JavaSparkContext sc) {
		List<Coordinate> p = sc.parallelize(Arrays.asList(c)).repartition(1).collect();
		HashSet<Coordinate> h = new HashSet<Coordinate>();
		h.addAll(p);
		p = new ArrayList<Coordinate>();
		p.addAll(h);
		Collections.sort(p);
		return sc.parallelize(Points.getPoints(p)).repartition(1);
	}

	public String toString() {
		return x + "," + y;
	}

	public int compareTo(Points p) {
		if (this.x == p.x) {
			Double y = this.y;
			Double y2 = p.y;
			return y.compareTo(y2);
		} else {
			Double x = this.x;
			Double x2 = p.x;
			return x.compareTo(x2);
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj instanceof Points) {
			Points p = (Points) obj;
			return (this.x == p.x && this.y == p.y);
		} else
			return false;
	}
}
