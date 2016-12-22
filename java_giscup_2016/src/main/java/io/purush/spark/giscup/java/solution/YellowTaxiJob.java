package io.purush.spark.giscup.java.solution;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.SystemClock;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by pswaminathan on 12/21/16.
 */
public class YellowTaxiJob {
    // Function to split the csv field and return only those columns we need-> 1,5,6
    // Also round out the lat,long fields and take only day field from datetime field
    static transient Function<String[], Tuple3<Integer, Double, Double>> splitFilter = (s) ->
            new Tuple3<>(Integer.parseInt(s[1].split("\\s")[0].split("-")[2]),
                    Double.parseDouble(s[5]),
                    Double.parseDouble(s[6]));

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage -> spark-submit --master <SPARK_URL> --class io.purush.spark.giscup.java.solution.YellowTaxiJob java_giscup_2016-1.0-SNAPSHOT.jar <HDFSPATH> <OUTPUTFILE>");

        }
        String textFile = args[0];

        SparkConf conf = new SparkConf().setAppName("giscup_solver");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> csvFileRDD = sc.textFile(textFile);

        // Remove csv header from RDD
        final String first = csvFileRDD.first();
        JavaRDD<String> headerRemovedRDD = csvFileRDD.filter(
                x -> !x.equals(first)
        );


        // Apply splitting, filtering and rounding on headerRemovedRDD
        JavaRDD<Tuple3<Integer, Double, Double>> roundedRDD = headerRemovedRDD.map(x -> splitFilter.apply(x.split(",")));

        // Filter out envelope around NYC in Long,Lat values
        JavaRDD<Tuple3<Integer, Double, Double>> evenelopeFilteredRDD = roundedRDD.filter(
                x -> x._3() >= 40.50 &&
                        x._3() <= 40.90 &&
                        x._2() >= -74.25 &&
                        x._2() <= -73.2

        );
        evenelopeFilteredRDD.take(1).forEach(System.out::println);

    }
}
