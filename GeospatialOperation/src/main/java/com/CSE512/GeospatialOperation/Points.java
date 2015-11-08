package com.CSE512.GeospatialOperation;

import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;

public class Points {
	public double x, y;

	public Points(double x, double y) {
		this.x = x;
		this.y = y;
	}
	
	public static ArrayList<Points> getPoints(List<Coordinate> coords){
		ArrayList<Points> p = new ArrayList<Points>();
		for (Coordinate c : coords) {
			p.add(new Points(c.x,c.y));
		}
		return p;
	}
	public String toString(){
		return "("+x+","+y+")";
	}

}
