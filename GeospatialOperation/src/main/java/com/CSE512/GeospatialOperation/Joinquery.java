package com.CSE512.GeospatialOperation;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

public class Joinquery {
	public static void main(String[] args) {
		String InputLocation1 = "hdfs://master:54310/data/JoinQueryInput1.csv";
		String InputLocation2 = "hdfs://master:54310/data/JoinQueryInput2.csv";
		String InputLocation3 = "hdfs://master:54310/data/JoinQueryInput3.csv";
		String OutputLocation = "hdfs://master:54310/data/JoinQuery";
		JavaSparkContext javasc = SContext.getJavaSparkContext();
		JavaRDD<String> inputFile1 = javasc.textFile(InputLocation1);
		JavaRDD<String> inputFile2 = javasc.textFile(InputLocation2);
		final List<String> target = inputFile1.collect();

		JavaRDD<String> out = inputFile2.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;

			public String call(String window) {
				{

					String windowpoints[] = window.split(",");
					String bid = windowpoints[0];
					String output = bid;
					double p = Double.parseDouble(windowpoints[1]);
					double r = Double.parseDouble(windowpoints[2]);
					double q = Double.parseDouble(windowpoints[3]);
					double s = Double.parseDouble(windowpoints[4]);
					Envelope ewindow = new Envelope(p, q, r, s);

					for (String part : target) {
						String targetpoints[] = part.split(",");

						if (targetpoints.length == 5) {
							double x1 = Double.parseDouble(targetpoints[1]);
							double y1 = Double.parseDouble(targetpoints[2]);
							double x2 = Double.parseDouble(targetpoints[3]);
							double y2 = Double.parseDouble(targetpoints[4]);
							Envelope rect = new Envelope(x1, x2, y1, y2);
							if (ewindow.contains(rect) || ewindow.intersects(rect)) {
								output = output + "," + targetpoints[0];
							}
						} else {
							double x1 = Double.parseDouble(targetpoints[1]);
							double y1 = Double.parseDouble(targetpoints[2]);
							Coordinate c = new Coordinate(x1, y1);
							if (ewindow.contains(c)) {
								output = output + "," + targetpoints[0];
							}
						}

					}

					return output;
				}
			}
		}).repartition(1);

		out.saveAsTextFile(OutputLocation);
		javasc.stop();
		javasc.close();
	}
}
