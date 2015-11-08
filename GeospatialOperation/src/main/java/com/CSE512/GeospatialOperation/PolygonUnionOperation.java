package com.CSE512.GeospatialOperation;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;


public class PolygonUnionOperation {
	
	
	public static void main(String args[]) throws IOException{
		
		String InputLocation = "hdfs://master:54310/data/PolygonUnionTestData.csv";
		String OutputLocation = "hdfs://master:54310/data/PolygonUnion";
		String localIntermediateFile = "hdfs://master:54310/data/PolygonUnionIntermediateFile";
	JavaSparkContext sc = SContext.getJavaSparkContext();

		//Read a text file from HDFS and return it as an RDD of Strings.
		JavaRDD<String> inputFile1 = sc.textFile(InputLocation);
		JavaRDD<Geometry> localUnionPolygon = inputFile1.mapPartitions(new LocalUnionOperation());
		
		localUnionPolygon.saveAsTextFile(localIntermediateFile);
		
		//Return a new RDD that has exactly 1 partition.
		JavaRDD<Geometry> partionList = localUnionPolygon.repartition(1);
		JavaRDD<Geometry> globalUnionPolygon = partionList.mapPartitions(new GlobalUnionOperation());
		List<Geometry> globalUnionPolygonList = globalUnionPolygon.collect();
		List<Points> globalUnionPolygonPointsList = new ArrayList<Points>();
		for(int i = 0; i < globalUnionPolygonList.size(); i++) {
			globalUnionPolygonPointsList.addAll(Points.getPoints(Arrays.asList(globalUnionPolygonList.get(0).getCoordinates())));
		}
		JavaRDD<Points> globalUnionPolygonPoints = sc.parallelize(globalUnionPolygonPointsList).repartition(1);
		globalUnionPolygonPoints.saveAsTextFile(OutputLocation);	
		sc.close();
	
	}
	
	/*public static void writeFile(JavaRDD<Geometry> globalUnionPolygon) throws IOException{
		String localIntermediateFile2 = "hdfs://master:54310/data/PolygonUnionIntermediateFile2";
		String OutputLocation = "hdfs://master:54310/data/PolygonUnion";
		File file = new File(localIntermediateFile2);


		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw); 
		bw.write(globalUnionPolygon.toString());
		bw.close();
		BufferedReader br = null;
		String sCurrentLine;

		br = new BufferedReader(new FileReader(file));

while ((sCurrentLine = br.readLine()) != null) {
			
			if(sCurrentLine.contains("POLYGON ((")){
				sCurrentLine=sCurrentLine.replace("POLYGON ((", "");
			}
			if(sCurrentLine.contains("))")){
				sCurrentLine=sCurrentLine.replace("))", "");
			}
			if(sCurrentLine.contains(", ")){
				sCurrentLine=sCurrentLine.replace(", ","\n");
			}
			 
			 if(sCurrentLine.contains(" ")){
				sCurrentLine=sCurrentLine.replace(" ", ",");
			}
			
		
		
		
		
	}*/

	

}

class LocalUnionOperation implements Serializable,FlatMapFunction<Iterator<String>, Geometry> 
{
	
	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<String> coorinatesList)
	{	
		GeometryFactory geoFactoryObject = new GeometryFactory();
		List<Geometry> currentRectangles = new ArrayList<Geometry>();
		
		while(coorinatesList.hasNext())
		{
			String coordinates = coorinatesList.next();
			
			String[] CoordList = coordinates.split(",");
			System.out.println(CoordList.length);
			for(int i=0;i<CoordList.length;i++){
				System.out.println("Coordinate::"+CoordList[i]);
			}
			
			Double x1 = Double.parseDouble(CoordList[0]);
			Double y1 = Double.parseDouble(CoordList[1]);
			Double x2 = Double.parseDouble(CoordList[2]);
			Double y2 = Double.parseDouble(CoordList[3]);
			
			// Other 2 points of rectangle.
			Coordinate q1=new Coordinate(x1,y1);
			Coordinate q2=new Coordinate(x1,y2);
			Coordinate q3=new Coordinate(x2,y2);
			Coordinate q4=new Coordinate(x2,y1);
			
			
			Coordinate[] coords = new Coordinate[]{q1,q2,q3,q4,q1};
			
			Geometry rectangle = geoFactoryObject.createPolygon(coords);
			currentRectangles.add(rectangle);
		}
		
		Collection<Geometry> polygons = currentRectangles;
		CascadedPolygonUnion cascadepoly = new CascadedPolygonUnion(polygons);
		Geometry listOfPolygonsUnion = cascadepoly.union();
		List<Geometry> localRectangles = new ArrayList<Geometry>();
		
	
		for(int i=0;i<listOfPolygonsUnion.getNumGeometries();i++)
		{
			Geometry listOfGemoetryN=listOfPolygonsUnion.getGeometryN(i);
			Geometry newRectangles =  listOfGemoetryN;
			localRectangles.add(newRectangles);
		}
		return localRectangles;
	}
}


class GlobalUnionOperation implements Serializable,FlatMapFunction<Iterator<Geometry>, Geometry> 
{
	
	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<Geometry> globalUnionData)
	{	
		List<Geometry> listOfPolygons = new ArrayList<Geometry>();
		List<Geometry> globalPolygons = new ArrayList<Geometry>();
		while(globalUnionData.hasNext())
		{
			Geometry polygons = globalUnionData.next();
			listOfPolygons.add(polygons);
		}

		Collection<Geometry> polygons = listOfPolygons;
		CascadedPolygonUnion cascadepoly = new CascadedPolygonUnion(polygons);
		Geometry listOfPolygonsUnion = cascadepoly.union();
		

		for(int i=0;i< listOfPolygonsUnion.getNumGeometries();i++)
		{
			
			Geometry listOfGemoetryN=listOfPolygonsUnion.getGeometryN(i);
			Geometry newRectangles =  listOfGemoetryN;
			globalPolygons.add(newRectangles);
		}
		
		return globalPolygons;
	}

}
