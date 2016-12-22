package io.purush.spark.giscup.java.solution;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.purush.spark.giscup.java.solution.SerializableComparator.serialize;

/**
 * @author Purush Swaminathan
 */
public class YellowTaxiJob {
    // Function to split the csv field and return only those columns we need-> 1,5,6
    // Also round out the lat,long fields and take only day field from datetime field
    static transient Function<String[], Tuple3<Integer, Double, Double>> splitFilter = (s) ->
            new Tuple3<>(Integer.parseInt(s[1].split("\\s")[0].split("-")[2]),
                    Double.parseDouble(s[5]),
                    Double.parseDouble(s[6]));

    static transient SerializableComparator<Tuple3<Integer, Double, Double>> orderByLongitude = (x, y) -> x._2().compareTo(y._2());

    static transient SerializableComparator<Tuple3<Integer, Double, Double>> orderByLatitude = (x, y) -> x._3().compareTo(y._3());

    static transient BiFunction<Tuple3<Integer, Double, Double>, Tuple2<Integer, Integer>, Boolean> isInCell = (d, c) -> {
        Double d2 = d._2() * 100;
        Double d3 = d._3() * 100;

        return (c._1() <= d2 && d2 <= (c._1() + 1) && c._2() <= d3 && d3 <= (c._2 + 1));
    };
    static transient Function<Tuple2<Integer, Integer>, Boolean> checkTimeBounds = (x) -> x._2() >= 1 && x._2() <= 31;

    static transient Function3<Tuple2<Integer, Integer>, Integer, Integer, Double> getis = (x, xcells, ycells) -> {
        List<Tuple2<Integer,Integer>> neighs = ScalaHelper.findNeighbor(x._1(), x._2(), xcells, ycells).filter(
                z -> );

    };

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
        JavaRDD<Tuple3<Integer, Double, Double>> envelopeFilteredRDD = roundedRDD.filter(
                x -> x._3() >= 40.50 &&
                        x._3() <= 40.90 &&
                        x._2() >= -74.25 &&
                        x._2() <= -73.2

        );

//        System.out.println("DEBUG: Line 86");
        // Find ranges to partition cells
        Double maxLongitude = envelopeFilteredRDD.max(serialize(orderByLongitude))._2();
//        System.out.println("DEBUG: Line 89");
        Double minLongitude = envelopeFilteredRDD.min(serialize(orderByLongitude))._2();
//        System.out.println("DEBUG: Line 91");
        Double maxLatitude = envelopeFilteredRDD.max(serialize(orderByLatitude))._3();
//        System.out.println("DEBUG: Line 93");
        Double minLatitude = envelopeFilteredRDD.min(serialize(orderByLatitude))._3();
//        System.out.println("DEBUG: Line 95");
        Integer xCells = (int) ((maxLongitude - minLongitude) * 100) + 1;
        Integer yCells = (int) ((maxLatitude - minLatitude) * 100) + 1;

        // Transform points relative to envelope borders (40.5,-74.25)
        JavaRDD<Tuple3<Integer, Double, Double>> spatiallyTransformedRDD = envelopeFilteredRDD.map(

                x -> new Tuple3<Integer, Double, Double>(
                        x._1(),
                        x._2() + Math.abs(minLongitude),
                        x._3() - Math.abs(minLatitude)
                )
        );

        // Create a cell-grid of ycells x xcells, flatten and zip to get row wise column order indices for any cell
        List<Tuple3<Integer, Integer, Integer>> zippedGrid = tabulateAndZipWithIndex(xCells + 1, yCells + 1);

        //Cellify spatiallyTransformedRDD
        JavaRDD<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>> spatialCelledRDD = spatiallyTransformedRDD.map(
                x -> new Tuple2<Tuple3<Integer, Integer, Integer>, Integer>(zippedGrid.stream().filter(
                        y -> isInCell.apply(x, new Tuple2<Integer, Integer>(y._1(), y._2()))
                ).collect(Collectors.toList()).get(0), x._1())
        );

        //spatialCelledRDD.take(1).forEach(System.out::println);

        // Make (CellId,Day) the key and filter those cells with array length=0
        JavaPairRDD<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Integer> keyedSpatialCelledRDD =
                spatialCelledRDD.mapToPair(
                        (PairFunction<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Integer>) t ->
                                new Tuple2<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Integer>(t, 1));

        JavaPairRDD<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Integer> reducedCelledRDD =
                keyedSpatialCelledRDD.reduceByKey((a, x) -> a + x);

        JavaRDD<Integer> xValues = reducedCelledRDD.map(
                Tuple2::_2
        );
        JavaRDD<Long> xSquareValues = xValues.map(
                Long::valueOf
        ).map(
                x -> x * x
        );

        // Common constants
        long n = xValues.count();
        double X = xValues.reduce((a, x) -> a + x) / (double) n;
        double sumXSq = xSquareValues.reduce((a, x) -> a + x) / (double) n;
        double XSq = X * X;
        double S = Math.sqrt(sumXSq - XSq);

        //HashMap the cells and their values
        // ******************COLLECT*********************
        Map<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Integer> cellValuesMap = reducedCelledRDD.collectAsMap();
        // ******************COLLECT*********************

    }

    private static List<Tuple3<Integer, Integer, Integer>> tabulateAndZipWithIndex(int xcells, int ycells) {
        List<Tuple3<Integer, Integer, Integer>> result = new ArrayList<>();

        for (int i = 0; i < xcells; i++) {
            for (int j = 0; j < ycells; j++) {
                int zip = i * ycells + j;
                result.add(new Tuple3<>(i, j, zip));

            }
        }
        return result;
    }
}





