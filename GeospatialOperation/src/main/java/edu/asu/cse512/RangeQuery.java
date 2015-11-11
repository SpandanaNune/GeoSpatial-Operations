package edu.asu.cse512;
/*
 * @author Kunal Lakhwani
 * @author Sayali Gole
 */
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class RangeQuery {

	public static void main(String[] args) {
		String InputLocation1 = args[0];
		String InputLocation2 = args[1];
		String OutputLocation = args[2];
		
		String finalIDs = "";
		SparkConf  conf  =  new  SparkConf (). setAppName ( "Group25-RangeQuery" );  
		JavaSparkContext  sc  =  new  JavaSparkContext ( conf ); 
		
		JavaRDD<String> inputFile2 = sc.textFile(InputLocation1); // points
		JavaRDD<String> inputFile1 = sc.textFile(InputLocation2); // query rectangle
																	

		String val = inputFile1.first();
		String[] windowPoints = val.split(",");
		final Double[] queryWindow = new Double[4];
		for (int i = 0; i < queryWindow.length; i++) {
			queryWindow[i] = Double.parseDouble(windowPoints[i]);
		}

		Broadcast<Double[]> broadcast = sc.broadcast(queryWindow);
		final Double[] value = broadcast.value();

		JavaPairRDD<String, String> result = inputFile2
				.mapToPair(new PairFunction<String, String, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(String data) {
						String parts[] = data.split(",");
						double id = Double.parseDouble(parts[0]);
						double x = Double.parseDouble(parts[1]);
						double y = Double.parseDouble(parts[2]);

						if ((value[0] <= x) && (value[1] <= y)
								&& (value[2] >= x) && (value[3] >= y)) {

							return new Tuple2<String, String>(String
									.valueOf(id), "null");
						}

						else
							return new Tuple2<String, String>("NULL", "b");
					}
				});
		List<Tuple2<String, String>> output = result.collect();
		List<String> list = new ArrayList<String>();
		
		for (Tuple2<?, ?> tuple : output) {
			if (!tuple._1().toString().contains("NULL")) {
				finalIDs += tuple._1() + "\n";
			}
		}
		
		list.add(finalIDs);
		JavaRDD<String> out = sc.parallelize(list).repartition(1);
		out.saveAsTextFile(OutputLocation);
		sc.close();
	}

}