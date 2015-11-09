package com.CSE512.GeospatialOperation;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Joinquery {
	public static void main(String[] args) {
		String InputLocation1 = "hdfs://master:54310/data/JoinQueryInput1.csv";
		String InputLocation2 = "hdfs://master:54310/data/JoinQueryInput1.csv";
		String OutputLocation = "hdfs://master:54310/data/JoinQuery";
		JavaSparkContext javasc = SContext.getJavaSparkContext();
		String[] value;
		JavaRDD<String> inputFile1 = javasc.textFile(InputLocation1);
		JavaRDD<String> inputFile2 = javasc.textFile(InputLocation2);
		List<String> list = inputFile1.collect();
		String[] data = list.toArray(new String[0]);
		Broadcast<String[]> br = javasc.broadcast(data);
		value = br.value();
		final String target[] = value;

		JavaPairRDD<String, String> out = inputFile2.mapToPair(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(String window) {
				double p = 0.0;
				double q = 0.0;
				double r = 0.0;
				double s = 0.0;
				String output = "";
				String bid = "";
				String windowpoints[] = window.split(",");
				bid = windowpoints[0];

				p = Double.parseDouble(windowpoints[1]);
				r = Double.parseDouble(windowpoints[2]);
				q = Double.parseDouble(windowpoints[3]);
				s = Double.parseDouble(windowpoints[4]);

				for (String part : target) {
					String targetpoints[] = part.split(",");

					if (targetpoints.length == 5) {
						
						double x1 = Double.parseDouble(targetpoints[1]);
						double y1 = Double.parseDouble(targetpoints[2]);
						double x2 = Double.parseDouble(targetpoints[3]);
						double y2 = Double.parseDouble(targetpoints[4]);

						if ((Math.max(p, q) > Math.max(x1, x2)) && (Math.max(r, s) > Math.max(y1, y2))
								&& (Math.min(p, q) < Math.min(x1, x2)) && (Math.min(r, s) < Math.min(y1, y2))) {
							output = output + "[" + targetpoints[0] + "]";
						}
					} else {
						double x1 = Double.parseDouble(targetpoints[1]);
						double y1 = Double.parseDouble(targetpoints[2]);
						if ((Math.max(p, q) > x1) && (Math.max(r, s) > y1) && (Math.min(p, q) < x1)
								&& (Math.min(r, s) < y1)) {
							output = output + "[" + targetpoints[0] + "]";
						}
					}

				}

				return new Tuple2<String, String>(bid, output);
			}
		}).repartition(1);

		out.saveAsTextFile(OutputLocation);
		javasc.stop();
		javasc.close();
	}
}
