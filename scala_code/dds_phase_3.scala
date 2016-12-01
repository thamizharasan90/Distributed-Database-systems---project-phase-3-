
object Cells {
  val textFile = "yellow_tripdata_2015-01_small.csv"

  /* ... new cell ... */

  val csvFileRDD = sc textFile textFile
  csvFileRDD.count

  /* ... new cell ... */

  val first = csvFileRDD.first
  val filteredCSVRDD = csvFileRDD filter {x => x!=first }

  /* ... new cell ... */

  def roundUp(d:String,i:Int): Double = {
    
    (d.substring(0,i).toDouble)
  }
  def splitFilter(s:Array[String]): (Int, Double, Double) = {
    var result:(Int, Double, Double) = null;
    def reRound(x:Int,i:Int):Double = if(s(x).length>i) roundUp(s(x),i) else s(x).toDouble;
   (s(1).split(" ")(0).split("-")(2).toInt, reRound(5,6), reRound(6,7))
  }

  /* ... new cell ... */

  val roundedRDD = filteredCSVRDD.map( x => (splitFilter(x.split(","))))

  /* ... new cell ... */

  val noZeroesRDD = roundedRDD filter { x =>  x._3 > 40.50 && x._3 < 40.90 && x._2 > -74.25 && x._2 < -73.70} 
  val orderByLongitude = new Ordering[(Int,Int,Int)]() {
    override def compare(x: (Int,Int, Int), y: (Int,Int,Int)): Int = 
      x._3 compare y._3
  }
  val orderByLatitude = new Ordering[(Int,Int,Int)]() {
    override def compare(x: (Int, Int,Int), y: (Int,Int,Int)): Int = 
      x._2 compare y._2
  }
  val intedRDD = noZeroesRDD map { x => (x._1, (x._2*1000).toInt, (x._3*1000).toInt)}
  val intedRDDOf1 = intedRDD.take(1)
  val maxLongitude = intedRDD.max()(orderByLongitude)._3
  val minLongitude = intedRDD.min()(orderByLongitude)._3
  val maxLatitude = intedRDD.max()(orderByLatitude)._2
  val minLatitude = intedRDD.min()(orderByLatitude)._2
  val xcells = (maxLongitude-minLongitude)/10
  val ycells = (maxLatitude-minLatitude)/10

  /* ... new cell ... */

  val rangeX = minLongitude to maxLongitude by 1
  val cutRDD = intedRDD map { x => (x._1, x._2+74250, x._3-40500) }
  cutRDD.count

  /* ... new cell ... */

  def isInCell(d:(Int,Int,Int), c:(Int,Int)): Boolean = {
      ((c._1)*10 <= d._2) &&
      (d._2 <=(c._1+1)*10-1) &&
      ((c._2)*10 <= d._3) && 
      (d._3 <= (c._2+1)*10-1)
    }

  /* ... new cell ... */

  val zippedGrid = Array.tabulate(ycells,xcells)( (x,y) => (x,y)).flatten.zipWithIndex

  /* ... new cell ... */

  val spatialCelledRDD = cutRDD map { x => ((zippedGrid filter { y => isInCell(x,y._1)}, x._1)) }

  /* ... new cell ... */

  val keyedSpatialCelledRDD = spatialCelledRDD.filter(x => x._1.length>0).map(x => ((x._1(0)._2, x._2),1))
  val reducedCelledRDD = keyedSpatialCelledRDD.reduceByKey(_ + _)
  val orderInt = new Ordering[((Int,Int),Int)]() {
    override def compare(x: ((Int,Int),Int), y: ((Int,Int),Int)): Int = 
      x._2 compare y._2
  }
  reducedCelledRDD.max()(orderInt)

  /* ... new cell ... */

  

  /* ... new cell ... */
}
                  