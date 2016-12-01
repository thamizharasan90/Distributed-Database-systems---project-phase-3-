package io.purush.spark.giscup.script

import org.apache.spark.{SparkConf, SparkContext}

object CellScript {
  def main(args: Array[String]): Unit = {

    // Setup csv file path on hdfs
    val textFile = "hdfs://localhost:54310/input"

    // Setup Spark Context
    val config = new SparkConf().setAppName("Phase-III-GISCUP")
    val sc = new SparkContext(config)

    //create RDD from csv file on hdfs
    val csvFileRDD = sc textFile textFile

    // Remove csv header from RDD
    val first = csvFileRDD.first
    val headerRemovedRDD = csvFileRDD filter { x => x != first }

    // Function to round out the double string -> longitude by i=5, latitude by i=6 to include - sign
    def roundUp(d: String, i: Int): Double = d.substring(0, i).toDouble

    // Function to split the csv field and return only those columns we need-> 1,5,6
    // Also round out the lat,long fields and take only day field from datetime field
    def splitFilter(s: Array[String]): (Int, Double, Double) = {
      def reRound(x: Int, i: Int): Double = if (s(x).length > i) roundUp(s(x), i) else s(x).toDouble
      (s(1).split(" ")(0).split("-")(2).toInt, reRound(5, 6), reRound(6, 7))
    }

    // Apply splitting, filtering and rounding on headerRemovedRDD
    val roundedRDD = headerRemovedRDD map {
      x => splitFilter(x.split(","))
    }

    // Filter out envelope around NYC in Long,Lat values
    val envelopeFilteredRDD = roundedRDD filter {
      x =>
        x._3 > 40.50 &&
          x._3 < 40.90 &&
          x._2 > -74.25 &&
          x._2 < -73.70
    }

    // Cutout longitude and compare it across tuple3s
    val orderByLongitude = new Ordering[(Int, Int, Int)]() {
      override def compare(x: (Int, Int, Int), y: (Int, Int, Int)): Int =
        x._3 compare y._3
    }

    // Cutout latitude and compare it across tuple3s
    val orderByLatitude = new Ordering[(Int, Int, Int)]() {
      override def compare(x: (Int, Int, Int), y: (Int, Int, Int)): Int =
        x._2 compare y._2
    }

    // Intify lat,longs, have an extra digit to make sure points are properly bucketed in cells
    val intedRDD = envelopeFilteredRDD map { x => (x._1, (x._2 * 1000).toInt, (x._3 * 1000).toInt) }

    // Find ranges to partition cells
    val maxLongitude = intedRDD.max()(orderByLongitude)._3
    val minLongitude = intedRDD.min()(orderByLongitude)._3
    val maxLatitude = intedRDD.max()(orderByLatitude)._2
    val minLatitude = intedRDD.min()(orderByLatitude)._2
    val xcells = (maxLongitude - minLongitude) / 10
    val ycells = (maxLatitude - minLatitude) / 10


    // Transform points relative to envelope borders (40.5,-74.25)
    val spatiallyTransformedRDD = intedRDD map { x => (x._1, x._2 + 74250, x._3 - 40500) }


    def isInCell(d: (Int, Int, Int), c: (Int, Int)): Boolean = {
      (c._1 * 10 <= d._2) &&
        (d._2 <= (c._1 + 1) * 10 - 1) &&
        (c._2 * 10 <= d._3) &&
        (d._3 <= (c._2 + 1) * 10 - 1)
    }

    // Create a cell-grid of ycells x xcells, flatten and zip to get row wise column order indices for any cell
    val zippedGrid = Array.tabulate(ycells, xcells)((x, y) => (x, y)).flatten.zipWithIndex

    // Cellify cutRDD
    val spatialCelledRDD = spatiallyTransformedRDD map { x => (zippedGrid filter { y => isInCell(x, y._1) }, x._1) }

    // Make (CellId,Day) the key and filter those cells with array length=0
    val keyedSpatialCelledRDD = spatialCelledRDD filter {
      x => x._1.length > 0
    } map {
      x => ((x._1(0)._2, x._2), 1)
    }
    println(keyedSpatialCelledRDD.count + " records in total before reducing cells downto grouped cells.")
    // ReduceByKey and wordCount -> trips in cell obtained!
    val reducedCelledRDD = keyedSpatialCelledRDD.reduceByKey(_ + _)

    val orderInt = new Ordering[((Int, Int), Int)]() {
      override def compare(x: ((Int, Int), Int), y: ((Int, Int), Int)): Int =
        x._2 compare y._2
    }
    println(reducedCelledRDD.map(x=>x._2).reduce(_+_) + " sum over cells")
    keyedSpatialCelledRDD.saveAsTextFile("hdfs://localhost:54310/cellGridWTrips")
    //reducedCelledRDD.max()(orderInt)

  }

}
                  