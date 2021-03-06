package edu.asu.cse512;

/**
 * @author Sayali Gole
 */
import java.io.IOException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

public class Union {

	public static void main(String args[]) throws IOException {

		String InputLocation = args[0];
		String OutputLocation = args[1];
		SparkConf  conf  =  new  SparkConf (). setAppName ( "Group25-PolygonUnion" );  
		JavaSparkContext  sc  =  new  JavaSparkContext ( conf ); 

		// Read a text file from HDFS and return it as an RDD of Strings.
		JavaRDD<String> inputFile1 = sc.textFile(InputLocation);
		JavaRDD<Geometry> localUnionPolygon = inputFile1.mapPartitions(new LocalUnionOperation());

		// Return a new RDD that has exactly 1 partition.
		JavaRDD<Geometry> partionList = localUnionPolygon.distinct().repartition(1);
		JavaRDD<Geometry> globalUnionPolygon = partionList.mapPartitions(new GlobalUnionOperation());

		List<Geometry> globalUnionPolygonList = globalUnionPolygon.collect();
		List<Coordinate> globalUnionPolygonCoordinateList = new ArrayList<Coordinate>();
		for (int i = 0; i < globalUnionPolygonList.size(); i++) {
			globalUnionPolygonCoordinateList.addAll(Arrays.asList(globalUnionPolygonList.get(i).getCoordinates()));
		}

		JavaRDD<Points> globalUnionPolygonPoints = Points.sortAndRemoveDuplicates(
				globalUnionPolygonCoordinateList.toArray(new Coordinate[globalUnionPolygonCoordinateList.size()]), sc);
		globalUnionPolygonPoints.distinct().saveAsTextFile(OutputLocation);
		sc.close();

	}

}

class LocalUnionOperation implements Serializable, FlatMapFunction<Iterator<String>, Geometry> {

	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<String> coorinatesList) {
		GeometryFactory geoFactoryObject = new GeometryFactory();
		List<Geometry> currentRectangles = new ArrayList<Geometry>();

		while (coorinatesList.hasNext()) {
			String coordinates = coorinatesList.next();

			String[] CoordList = coordinates.split(",");
			System.out.println(CoordList.length);
			for (int i = 0; i < CoordList.length; i++) {
				System.out.println("Coordinate::" + CoordList[i]);
			}

			Double x1 = Double.parseDouble(CoordList[0]);
			Double y1 = Double.parseDouble(CoordList[1]);
			Double x2 = Double.parseDouble(CoordList[2]);
			Double y2 = Double.parseDouble(CoordList[3]);

			// Other 2 points of rectangle.
			Coordinate q1 = new Coordinate(x1, y1);
			Coordinate q2 = new Coordinate(x1, y2);
			Coordinate q3 = new Coordinate(x2, y2);
			Coordinate q4 = new Coordinate(x2, y1);

			Coordinate[] coords = new Coordinate[] { q1, q2, q3, q4, q1 };

			Geometry rectangle = geoFactoryObject.createPolygon(coords);
			currentRectangles.add(rectangle);
		}

		Collection<Geometry> polygons = currentRectangles;
		CascadedPolygonUnion cascadepoly = new CascadedPolygonUnion(polygons);
		Geometry listOfPolygonsUnion = cascadepoly.union();
		List<Geometry> localRectangles = new ArrayList<Geometry>();

		for (int i = 0; i < listOfPolygonsUnion.getNumGeometries(); i++) {
			Geometry listOfGemoetryN = listOfPolygonsUnion.getGeometryN(i);
			Geometry newRectangles = listOfGemoetryN;
			localRectangles.add(newRectangles);
		}
		return localRectangles;
	}
}

class GlobalUnionOperation implements Serializable, FlatMapFunction<Iterator<Geometry>, Geometry> {

	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<Geometry> globalUnionData) {
		List<Geometry> listOfPolygons = new ArrayList<Geometry>();
		List<Geometry> globalPolygons = new ArrayList<Geometry>();
		while (globalUnionData.hasNext()) {
			Geometry polygons = globalUnionData.next();
			listOfPolygons.add(polygons);
		}

		Collection<Geometry> polygons = listOfPolygons;
		CascadedPolygonUnion cascadepoly = new CascadedPolygonUnion(polygons);
		Geometry listOfPolygonsUnion = cascadepoly.union();

		for (int i = 0; i < listOfPolygonsUnion.getNumGeometries(); i++) {

			Geometry listOfGemoetryN = listOfPolygonsUnion.getGeometryN(i);
			Geometry newRectangles = listOfGemoetryN;
			if (!globalPolygons.contains(newRectangles))
				globalPolygons.add(newRectangles);
		}

		return globalPolygons;
	}

}
