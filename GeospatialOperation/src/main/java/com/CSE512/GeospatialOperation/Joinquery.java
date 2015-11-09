package com.CSE512.GeospatialOperation;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

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

					for (String part : target) {
						String targetpoints[] = part.split(",");

						if (targetpoints.length == 5) {
							double x1 = Double.parseDouble(targetpoints[1]);
							double y1 = Double.parseDouble(targetpoints[2]);
							double x2 = Double.parseDouble(targetpoints[3]);
							double y2 = Double.parseDouble(targetpoints[4]);

							if (Math.max(p, q) >= Math.max(x1, x2) && Math.max(r, s) >= Math.max(y1, y2)
									&& Math.min(p, q) <= Math.min(x1, x2) && Math.min(r, s) <= Math.min(y1, y2)) {

								output = output + "," + targetpoints[0];

							}
						} else {
							double x1 = Double.parseDouble(targetpoints[1]);
							double y1 = Double.parseDouble(targetpoints[2]);
							if (((Math.max(p, q) >= x1) && (Math.max(r, s) >= y1))
									&& ((Math.min(p, q) <= x1) && (Math.min(r, s) <= y1))) {

								output = output + "," + targetpoints[0];
							}
						}

					}

					return output;
				}
			}
		});

		out.saveAsTextFile(OutputLocation);
		javasc.stop();
		javasc.close();
	}
}
